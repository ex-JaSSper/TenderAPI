"""
Tender Loader API + Parser
Version: 2.2.1 (with publish date timezone fix)
"""

import os
import re
import json
import logging
import pytz  # 🔧 ДЛЯ РАБОТЫ С ЧАСОВЫМИ ПОЯСАМИ
from datetime import datetime, timedelta, time as dt_time
from typing import Optional, List, Dict, Any
from io import BytesIO

import requests
from fastapi import FastAPI, HTTPException, File, UploadFile, BackgroundTasks
from fastapi.responses import JSONResponse
import gspread
from gspread.exceptions import SpreadsheetNotFound
from mammoth import convert_to_html
from docx import Document

# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Tender Loader API + Parser", version="2.2.1")

API_TOKEN = os.getenv("API_TOKEN")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_CREDS_B64 = os.getenv("GOOGLE_CREDS_B64")
TIMEZONE = os.getenv("TIMEZONE", "Asia/Novosibirsk")

MAX_FILE_SIZE_MB = 100
DOWNLOAD_TIMEOUT_SEC = 120
PARSE_TIMEOUT_SEC = 60

EXCLUDED_PLACING_WAYS = {15}

PLACING_WAYS = {
    15: "Электронный аукцион", 3: "Открытый аукцион (ЭФ)", 12: "Закрытый аукцион",
    22: "Запрос котировок ЭФ", 23: "Открытый конкурс ЭФ", 24: "Запрос предложений ЭФ",
    25: "Конкурс с ограничением ЭФ", 26: "Двухэтапный ЭФ",
}

# ============================================================
# ФУНКЦИИ РАБОТЫ С ЧАСОВЫМИ ПОЯСАМИ
# ============================================================

def get_local_timezone():
    return pytz.timezone(TIMEZONE)

def get_target_date():
    local_tz = get_local_timezone()
    now = datetime.now(local_tz)
    return (now - timedelta(days=1)).date()

def get_date_range_timestamps(target_date):
    local_tz = get_local_timezone()
    start_time = local_tz.localize(datetime.combine(target_date, dt_time.min))
    end_time = local_tz.localize(datetime.combine(target_date, dt_time.max))
    start_timestamp = int(start_time.timestamp() * 1000)
    end_timestamp = int(end_time.timestamp() * 1000)
    return start_timestamp, end_timestamp, start_time, end_time

# ⭐️ НОВАЯ ФУНКЦИЯ ДЛЯ КОНВЕРТАЦИИ ДАТЫ ПУБЛИКАЦИИ
def format_publish_date_to_local(publish_date: str) -> str:
    """Преобразует ISO-дату из TenderPlan (UTC) в строку по локальному времени."""
    if not publish_date:
        return ""
    try:
        # Пример формата: '2025-12-09T17:30:00Z'
        dt_utc = datetime.fromisoformat(publish_date.replace("Z", "+00:00"))
        dt_local = dt_utc.astimezone(get_local_timezone())
        return dt_local.strftime("%d.%m.%Y %H:%M")
    except Exception:
        # Если формат неожиданный, возвращаем как есть
        return publish_date

# ============================================================
# ФУНКЦИИ ФИЛЬТРАЦИИ И GOOGLE SHEETS
# ============================================================

def should_skip_tender(placing_way: int) -> bool:
    return placing_way in EXCLUDED_PLACING_WAYS

def get_google_sheets_client():
    if not GOOGLE_CREDS_B64:
        raise ValueError("GOOGLE_CREDS_B64 not set")
    import base64
    creds_json = base64.b64decode(GOOGLE_CREDS_B64).decode('utf-8')
    creds_dict = json.loads(creds_json)
    return gspread.service_account_from_dict(creds_dict)

def get_or_create_worksheet(gc, spreadsheet_id: str, worksheet_name: str):
    try:
        spreadsheet = gc.open_by_key(spreadsheet_id)
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)
            headers = ["ID", "Название", "Закупщик", "Начальная цена", "Способ размещения", "Статус", "Дата публикации", "Ссылка"]
            worksheet.insert_row(headers, index=1)
        return worksheet
    except SpreadsheetNotFound:
        logger.error(f"Spreadsheet {spreadsheet_id} not found")
        raise

# ... (остальные эндпоинты /ping, /health, /check-time, /info, /parse-doc остаются без изменений) ...
# ============================================================
# ENDPOINTS: ЗДОРОВЬЕ И ИНФО (без изменений)
# ============================================================

@app.get("/ping")
def ping():
    return {"status": "ok", "message": "API is running"}


@app.get("/health")
def health_check():
    try:
        if GOOGLE_CREDS_B64:
            gc = get_google_sheets_client()
            return {"status": "ok", "api": "running", "google_sheets": "connected", "version": "2.2.1"}
        else:
            return {"status": "warning", "api": "running", "google_sheets": "not configured", "version": "2.2.1"}
    except Exception as e:
        return {"status": "error", "api": "running", "google_sheets": "failed", "error": str(e), "version": "2.2.1"}


@app.get("/check-time")
def check_time():
    try:
        local_tz = get_local_timezone()
        now_utc = datetime.now(pytz.UTC)
        now_local = datetime.now(local_tz)
        target_date = get_target_date()
        start_ts, end_ts, start_time, end_time = get_date_range_timestamps(target_date)

        return {
            "status": "ok",
            "timezone": TIMEZONE,
            "current_time": {
                "system": datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
                "utc": now_utc.strftime("%d.%m.%Y %H:%M:%S UTC"),
                "local": now_local.strftime("%d.%m.%Y %H:%M:%S %z (UTC%z)")
            },
            "target_date": {
                "date": target_date.strftime("%d.%m.%Y"),
                "start": {"local": start_time.strftime("%d.%m.%Y %H:%M:%S %z"), "utc": start_time.astimezone(pytz.UTC).strftime("%d.%m.%Y %H:%M:%S UTC"), "timestamp": start_ts},
                "end": {"local": end_time.strftime("%d.%m.%Y %H:%M:%S %z"), "utc": end_time.astimezone(pytz.UTC).strftime("%d.%m.%Y %H:%M:%S UTC"), "timestamp": end_ts}
            },
            "message": f"Будут загружены тендеры за {target_date.strftime('%d.%m.%Y')} (вчерашний день в {TIMEZONE})"
        }
    except Exception as e:
        logger.error(f"Error in check_time: {e}", exc_info=True)
        return {"status": "error", "error": str(e), "timezone": TIMEZONE}


@app.get("/info")
def get_info():
    return {
        "app": "Tender Loader API + Parser",
        "version": "2.2.1",
        "config": {
            "timezone": TIMEZONE,
            "max_file_size_mb": MAX_FILE_SIZE_MB,
            "download_timeout_sec": DOWNLOAD_TIMEOUT_SEC,
            "parse_timeout_sec": PARSE_TIMEOUT_SEC,
            "excluded_placing_ways": list(EXCLUDED_PLACING_WAYS),
            "excluded_placing_ways_details": {str(pw): PLACING_WAYS.get(pw, "Unknown") for pw in EXCLUDED_PLACING_WAYS}
        },
        "endpoints": {
            "GET /ping": "Health check", "GET /health": "Detailed service check",
            "GET /check-time": "Check timezones", "POST /parse-doc": "Parse DOC/DOCX",
            "GET /load-tenders": "Load tenders", "GET /errors": "View errors log", "GET /info": "API info"
        },
        "improvements": {
            "publish_date_fix": "✅ Publish date is now converted to local timezone in sheets",
            "timezone": "✅ Converted to local timezone",
            "filtering": "✅ Exclude electronic auctions",
        }
    }

# ============================================================
# PARSE DOC ENDPOINT (без изменений)
# ============================================================

def parse_document(file_content: bytes, file_name: str) -> Dict[str, Any]:
    try:
        file_stream = BytesIO(file_content)
        if file_name.lower().endswith('.docx'):
            result = convert_to_html(file_stream)
            text = re.sub(r'<[^>]+>', '', result.value).strip()
        elif file_name.lower().endswith('.doc'):
            doc = Document(file_stream)
            text = '\n'.join([para.text for para in doc.paragraphs])
        else:
            raise ValueError("Unsupported file format")
        return {"status": "success", "file_name": file_name, "content_length": len(text), "preview": text[:500] if text else "No content"}
    except Exception as e:
        logger.error(f"Error parsing document: {e}")
        return {"status": "error", "file_name": file_name, "error": str(e)}

@app.post("/parse-doc")
async def parse_doc(file: UploadFile = File(...)):
    try:
        file_content = await file.read()
        if len(file_content) > MAX_FILE_SIZE_MB * 1024 * 1024:
            raise HTTPException(status_code=413, detail=f"File too large. Max size: {MAX_FILE_SIZE_MB}MB")
        result = parse_document(file_content, file.filename)
        return result
    except Exception as e:
        logger.error(f"Error in parse_doc: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# LOAD TENDERS ENDPOINT (ГЛАВНАЯ ФУНКЦИЯ С ИСПРАВЛЕНИЕМ)
# ============================================================

@app.get("/load-tenders")
def load_tenders():
    logger.info("=" * 60)
    logger.info("🔄 ЗАПУСК ЗАГРУЗКИ ТЕНДЕРОВ")
    logger.info("=" * 60)

    try:
        # ЭТАП 1: ПОДГОТОВКА
        logger.info("📋 ЭТАП 1: Подготовка параметров")
        if not API_TOKEN: raise ValueError("API_TOKEN not configured")

        local_tz = get_local_timezone()
        target_date = get_target_date()
        start_timestamp, end_timestamp, start_time, end_time = get_date_range_timestamps(target_date)

        logger.info(f"🎯 Целевой день: {target_date.strftime('%d.%m.%Y')} ({TIMEZONE})")
        logger.info(f"⏰ Диапазон: {start_time.strftime('%H:%M:%S')} - {end_time.strftime('%H:%M:%S')}")
        logger.info(f"🔧 Исключённые способы: {EXCLUDED_PLACING_WAYS}")

        # ЭТАП 2: ЗАГРУЗКА
        logger.info("📡 ЭТАП 2: Загрузка тендеров с TenderPlan API")
        url = "https://api.tenderplan.ru/api/v2/tenders"
        headers = {"Authorization": f"Bearer {API_TOKEN}"}
        all_tenders = []
        page = 1
        while True:
            params = {"per_page": 100, "page": page, "dateFrom": start_timestamp, "dateTo": end_timestamp}
            response = requests.get(url, headers=headers, params=params, timeout=DOWNLOAD_TIMEOUT_SEC)
            if response.status_code != 200:
                logger.error(f"API error: {response.status_code}"); break
            data = response.json()
            tenders = data.get("data", [])
            if not tenders: break
            all_tenders.extend(tenders)
            logger.info(f"Загружено {len(tenders)} тендеров со страницы {page}")
            page += 1
        logger.info(f"✅ Всего загружено: {len(all_tenders)} тендеров")

        # ЭТАП 3: GOOGLE SHEETS
        logger.info("📊 ЭТАП 3: Подключение к Google Sheets")
        gc = get_google_sheets_client()
        worksheet = get_or_create_worksheet(gc, GOOGLE_SHEET_ID, "Tenders")
        logger.info("✅ Подключение к Google Sheets успешно")

        # ЭТАП 4: ОБРАБОТКА (С ФИЛЬТРАЦИЕЙ И ИСПРАВЛЕНИЕМ ДАТЫ)
        logger.info("⚙️  ЭТАП 4: Обработка тендеров")
        rows, skipped_tenders = [], []
        for tender in all_tenders:
            placing_way = tender.get("placingWay", {}).get("id")
            if should_skip_tender(placing_way):
                skipped_tenders.append({"id": tender.get("id"), "placing_way": placing_way})
                continue

            # ⭐️ ИСПРАВЛЕНИЕ ЗДЕСЬ ⭐️
            row = [
                tender.get("id", ""),
                tender.get("name", "")[:100],
                tender.get("organization", {}).get("name", "")[:100],
                tender.get("price", ""),
                tender.get("placingWay", {}).get("name", ""),
                tender.get("status", {}).get("name", ""),
                format_publish_date_to_local(tender.get("publishDate", "")),  # <--- ИСПОЛЬЗУЕМ НОВУЮ ФУНКЦИЮ
                tender.get("url", "")
            ]
            rows.append(row)

        # ЭТАП 5: ДОБАВЛЕНИЕ В ТАБЛИЦУ
        logger.info(f"📝 ЭТАП 5: Добавление данных в таблицу ({len(rows)} тендеров)")
        if rows:
            worksheet.append_rows(rows, value_input_option="RAW")
            logger.info(f"✅ Добавлено {len(rows)} строк в Google Sheets")

        # ИТОГИ
        logger.info("=" * 60)
        logger.info("✅ ИТОГИ ОБРАБОТКИ:")
        logger.info(f"   Всего получено:      {len(all_tenders)}")
        logger.info(f"   Добавлено в таблицу: {len(rows)}")
        logger.info(f"   Пропущено:           {len(skipped_tenders)}")
        logger.info("=" * 60)

        return {
            "status": "success", "added": len(rows), "total_fetched": len(all_tenders),
            "skipped": len(skipped_tenders), "timestamp": datetime.now(local_tz).strftime("%d.%m.%Y %H:%M"),
            "target_date": target_date.strftime("%d.%m.%Y"), "timezone": TIMEZONE,
            "validation": {"match": len(rows) + len(skipped_tenders) == len(all_tenders)}
        }
    except Exception as e:
        logger.error(f"Fatal error in load_tenders: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}

# ============================================================
# STARTUP
# ============================================================
@app.on_event("startup")
async def startup_event():
    logger.info("=" * 60)
    logger.info("🚀 TENDER LOADER API STARTING")
    logger.info(f"Version: 2.2.1")
    logger.info(f"Timezone: {TIMEZONE}")
    logger.info(f"Excluded placing ways: {EXCLUDED_PLACING_WAYS}")
    logger.info("=" * 60)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
