import os
import logging
from fastapi import FastAPI
import requests
from datetime import datetime, timedelta, time as dt_time
import gspread
from dotenv import load_dotenv
from typing import List

# Загружаем настройки из .env
load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")

URL = "https://tenderplan.ru/api/tenders/v2/getlist"

# Проверки окружения
if not API_TOKEN:
    raise RuntimeError("В .env не найден API_TOKEN")
if not GOOGLE_SHEET_ID:
    raise RuntimeError("В .env не найден GOOGLE_SHEET_ID")
if not GOOGLE_CREDENTIALS_FILE:
    raise RuntimeError("В .env не найден GOOGLE_CREDENTIALS_FILE")

# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

app = FastAPI(title="Tender Loader API")

# Словарь способов размещения
PLACING_WAYS = {
    0: "Иной способ",
    1: "Открытый конкурс",
    2: "Открытый аукцион",
    3: "Открытый аукцион в электронной форме",
    4: "Запрос котировок",
    5: "Предварительный отбор",
    6: "Закупка у единственного поставщика",
    7: "Конкурс с ограниченным участием",
    8: "Двухэтапный конкурс",
    9: "Закрытый конкурс",
    10: "Закрытый конкурс с ограниченным участием",
    11: "Закрытый двухэтапный конкурс",
    12: "Закрытый аукцион",
    13: "Запрос котировок без размещения извещения",
    14: "Запрос предложений",
    15: "Электронный аукцион",
    16: "Иной многолотовый способ",
    17: "Сообщение о заинтересованности",
    18: "Иной однолотовый способ",
    19: "Редукцион",
    20: "Переторжка",
    21: "Конкурентные переговоры",
    22: "Запрос котировок в электронной форме",
    23: "Открытый конкурс в электронной форме",
    24: "Запрос предложений в электронной форме",
    25: "Конкурс с ограниченным участием в электронной форме",
    26: "Двухэтапный конкурс в электронной форме",
    27: "Запрос цен товаров, работ, услуг",
    28: "Голландский аукцион",
    29: "Публичное предложение",
    30: "Закупки малого объема"
}

def tender_ts(dt: datetime) -> int:
    """Перевод datetime в timestamp для API (в миллисекундах)"""
    return int(dt.timestamp() * 1000)

def convert_timestamp(ts):
    """Перевод timestamp из API в читаемую дату"""
    if ts:
        return datetime.fromtimestamp(ts / 1000).strftime('%d.%m.%Y %H:%M')
    return ""

def get_sheet():
    """Получаем объект листа Google Sheet"""
    client = gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)
    return client.open_by_key(GOOGLE_SHEET_ID).sheet1

def ensure_header(sheet):
    """Создаём заголовок, если его нет или он неправильный"""
    header = [
        "Дата строки",
        "Название",
        "Заказчик",
        "НМЦ",
        "Ссылка",
        "Дата публикации",
        "Дата окончания подачи",
        "Способ размещения"
    ]
    first_row = sheet.row_values(1)
    if first_row != header:
        if first_row:
            sheet.delete_rows(1)
        sheet.insert_row(header, 1)

@app.get("/load-tenders")
def load_tenders():
    """Выгрузка тендеров за вчерашний день"""
    now = datetime.now()
    target_day = now - timedelta(days=1)

    # Диапазон вчерашнего дня
    start_dt = datetime.combine(target_day, dt_time(0, 0))
    end_dt = datetime.combine(target_day, dt_time(23, 59, 59))

    from_ts = tender_ts(start_dt)
    to_ts = tender_ts(end_dt)

    logging.info(f"Загружаем тендеры за {target_day.strftime('%d.%m.%Y')}")

    headers = {"Authorization": f"Bearer {API_TOKEN}"}

    all_tenders = []
    page = 0

    while True:
        params = {
            "fromPublicationDateTime": from_ts,  # начало публикации
            "toPublicationDateTime": to_ts,      # конец публикации
            "statuses": "1",
            "page": page
        }

        resp = requests.get(URL, headers=headers, params=params)
        if resp.status_code != 200:
            return {"error": "TenderPlan API error", "status": resp.status_code}

        data = resp.json()
        tenders = data.get("tenders", [])

        if not tenders:
            break  # Останавливаем цикл, если нет данных

        all_tenders.extend(tenders)
        page += 1

    if not all_tenders:
        return {
            "message": "Нет тендеров за вчера",
            "yesterday": target_day.strftime("%d.%m.%Y")
        }

    sheet = get_sheet()
    ensure_header(sheet)

    rows = []
    now_str = now.strftime("%d.%m.%Y %H:%M")

    for t in all_tenders:
        customers = t.get("customers", [])
        customer_names = ", ".join([c.get("name", "") for c in customers])

        placing_id = t.get("placingWay")
        placing_name = PLACING_WAYS.get(placing_id, "Неизвестно")

        rows.append([
            now_str,
            t.get("orderName", ""),
            customer_names,
            t.get("maxPrice", ""),
            f"https://tenderplan.ru/app?key=0&tender={t.get('_id')}",
            convert_timestamp(t.get("publicationDateTime")),
            convert_timestamp(t.get("submissionCloseDateTime")),
            placing_name
        ])

    sheet.append_rows(rows, value_input_option="USER_ENTERED")

    return {
        "status": "ok",
        "rows_added": len(rows),
        "loaded_for_date": target_day.strftime("%d.%m.%Y")
    }
