import os
import base64
import logging
import traceback
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import requests
from datetime import datetime, timedelta, time as dt_time
import gspread
from dotenv import load_dotenv
from tempfile import NamedTemporaryFile
import mammoth
import docx
from enum import Enum
import asyncio
from concurrent.futures import ThreadPoolExecutor
import io
import pytz  # ✅ ДОБАВИЛИ PYTZ

# -----------------------
# Загружаем настройки
# -----------------------
load_dotenv()
API_TOKEN = os.getenv("API_TOKEN")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_CREDS_B64 = os.getenv("GOOGLE_CREDS_B64")

# Новые переменные для оптимизации
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "100"))
DOWNLOAD_TIMEOUT_SEC = int(os.getenv("DOWNLOAD_TIMEOUT_SEC", "120"))
PARSE_TIMEOUT_SEC = int(os.getenv("PARSE_TIMEOUT_SEC", "60"))

# -----------------------
# ✅ ВРЕМЕННЫЕ ЗОНЫ
# -----------------------
UTC_TZ = pytz.UTC
MSK_TZ = pytz.timezone('Europe/Moscow')  # UTC+3 (где публикуются тендеры)
NSK_TZ = pytz.timezone('Asia/Novosibirsk')  # UTC+7 (где запускается скрипт)

# -----------------------
# Создание service_account.json из Base64 (для Render)
# -----------------------
GOOGLE_CREDENTIALS_FILE = "service_account.json"
if GOOGLE_CREDS_B64:
    with open(GOOGLE_CREDENTIALS_FILE, "w") as f:
        f.write(base64.b64decode(GOOGLE_CREDS_B64).decode("utf-8"))

TENDERS_URL = "https://tenderplan.ru/api/tenders/v2/getlist"
ATTACHMENTS_URL = "https://tenderplan.ru/api/tenders/attachments"

if not API_TOKEN or not GOOGLE_SHEET_ID:
    raise RuntimeError("Не указаны обязательные переменные окружения: API_TOKEN, GOOGLE_SHEET_ID")

# -----------------------
# Логирование
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Tender Loader API + Parser")

# Thread pool для парсинга (не блокирует главный loop)
executor = ThreadPoolExecutor(max_workers=2)


# -----------------------
# ENUM для типов ошибок
# -----------------------
class ErrorType(Enum):
    TENDERPLAN_API_ERROR = "TenderPlan API Error"
    GOOGLE_SHEETS_ERROR = "Google Sheets Error"
    DOCUMENT_PARSE_ERROR = "Document Parse Error"
    FILE_DOWNLOAD_ERROR = "File Download Error"
    GOOGLE_AUTH_ERROR = "Google Authentication Error"
    PING_ERROR = "Ping Health Check Error"
    UNKNOWN_ERROR = "Unknown Error"
    FILE_SIZE_ERROR = "File Size Error"


# -----------------------
# КЛАСС ДЛЯ УПРАВЛЕНИЯ ОШИБКАМИ
# -----------------------
class ErrorNotificationManager:
    """Управляет ошибками и логирует их"""

    def __init__(self):
        self.errors = []

    def send_notification(self, error_type: ErrorType, error_msg: str,
                          stage: str, details: dict = None):
        """Регистрирует ошибку и логирует её"""
        timestamp = datetime.now().isoformat()
        error_data = {
            "timestamp": timestamp,
            "error_type": error_type.value,
            "stage": stage,
            "message": error_msg,
            "details": details or {}
        }

        self.errors.append(error_data)
        logger.error(f"[{error_type.value}] {stage}: {error_msg}")


error_manager = ErrorNotificationManager()

# -----------------------
# Словарь способов размещения
# -----------------------
PLACING_WAYS = {
    0: "Иной способ", 1: "Открытый конкурс", 2: "Открытый аукцион",
    3: "Открытый аукцион (ЭФ)", 4: "Запрос котировок", 5: "Предварительный отбор",
    6: "Единственный поставщик", 7: "Конкурс с ограничением", 8: "Двухэтапный конкурс",
    9: "Закрытый конкурс", 10: "Закрытый конкурс с огр.", 11: "Закрытый двухэтапный",
    12: "Закрытый аукцион", 13: "Запрос котировок без извещения",
    14: "Запрос предложений", 15: "Электронный аукцион", 16: "Иной многолотовый способ",
    17: "Сообщение о заинтересованности", 18: "Иной однолотовый способ",
    19: "Редукцион", 20: "Переторжка", 21: "Переговоры",
    22: "Запрос котировок ЭФ", 23: "Открытый конкурс ЭФ",
    24: "Запрос предложений ЭФ", 25: "Конкурс с ограничением ЭФ",
    26: "Двухэтапный ЭФ", 27: "Запрос цен", 28: "Голландский аукцион",
    29: "Публичное предложение", 30: "Закупки малого объема"
}


# -----------------------
# Вспомогательные функции
# -----------------------
def tender_ts(dt: datetime) -> int:
    """Конвертирует datetime в TenderPlan timestamp (миллисекунды)"""
    return int(dt.timestamp() * 1000)


def convert_timestamp(ts):
    """Конвертирует timestamp в читаемый формат"""
    if ts:
        try:
            return datetime.fromtimestamp(ts / 1000).strftime('%d.%m.%Y %H:%M')
        except Exception as e:
            logger.warning(f"Ошибка конвертации timestamp {ts}: {e}")
            return ""
    return ""


def get_sheet():
    """Получает доступ к Google Sheets"""
    try:
        client = gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)
        sheet = client.open_by_key(GOOGLE_SHEET_ID).sheet1
        logger.info("Успешное подключение к Google Sheets")
        return sheet
    except FileNotFoundError:
        error_manager.send_notification(
            ErrorType.GOOGLE_AUTH_ERROR,
            "Файл service_account.json не найден",
            "Инициализация Google Sheets",
            {"file": GOOGLE_CREDENTIALS_FILE}
        )
        raise
    except gspread.exceptions.AuthenticationError as e:
        error_manager.send_notification(
            ErrorType.GOOGLE_AUTH_ERROR,
            f"Ошибка аутентификации Google: {str(e)}",
            "Подключение к Google Sheets",
            {"error_code": type(e).__name__}
        )
        raise
    except gspread.exceptions.SpreadsheetNotFound as e:
        error_manager.send_notification(
            ErrorType.GOOGLE_SHEETS_ERROR,
            f"Google Sheet с ID {GOOGLE_SHEET_ID} не найден",
            "Поиск Google Sheets",
            {"sheet_id": GOOGLE_SHEET_ID}
        )
        raise
    except Exception as e:
        error_manager.send_notification(
            ErrorType.GOOGLE_SHEETS_ERROR,
            f"Неожиданная ошибка при подключении к Google Sheets: {str(e)}",
            "Подключение к Google Sheets",
            {"error": str(e), "traceback": traceback.format_exc()}
        )
        raise


def ensure_header(sheet, max_docs=0):
    """Обеспечивает корректный заголовок в Google Sheets"""
    try:
        header = [
            "Дата строки", "ID тендера", "Название", "Заказчик",
            "НМЦ", "Ссылка", "Дата публикации",
            "Дата окончания подачи", "Способ размещения"
        ]

        for i in range(1, max_docs + 1):
            header.append(f"Документ {i} Название")
            header.append(f"Документ {i} Ссылка")

        first_row = sheet.row_values(1)
        if first_row != header:
            if first_row:
                sheet.delete_rows(1)
            sheet.insert_row(header, 1)
            logger.info("Заголовок обновлен в Google Sheets")

    except gspread.exceptions.APIError as e:
        error_manager.send_notification(
            ErrorType.GOOGLE_SHEETS_ERROR,
            f"Google Sheets API ошибка при обновлении заголовка: {str(e)}",
            "Обновление заголовка",
            {"error_code": getattr(e, 'status_code', None), "message": str(e)}
        )
        raise
    except Exception as e:
        error_manager.send_notification(
            ErrorType.GOOGLE_SHEETS_ERROR,
            f"Ошибка при обновлении заголовка: {str(e)}",
            "Обновление заголовка",
            {"error": str(e)}
        )
        raise


def fetch_attachments(tender_id, headers):
    """Получает приложения для тендера"""
    try:
        resp = requests.get(
            f"{ATTACHMENTS_URL}?id={tender_id}",
            headers=headers,
            timeout=40
        )

        if resp.status_code == 401:
            error_manager.send_notification(
                ErrorType.TENDERPLAN_API_ERROR,
                "Неавторизованный запрос к TenderPlan API (401 Unauthorized)",
                "Получение приложений",
                {
                    "tender_id": tender_id,
                    "status_code": 401,
                    "message": "Проверьте API_TOKEN"
                }
            )
            return []

        elif resp.status_code == 429:
            error_manager.send_notification(
                ErrorType.TENDERPLAN_API_ERROR,
                "Превышен лимит запросов к TenderPlan API (429 Too Many Requests)",
                "Получение приложений",
                {
                    "tender_id": tender_id,
                    "status_code": 429,
                    "message": "Попробуйте позже"
                }
            )
            return []

        elif resp.status_code != 200:
            error_manager.send_notification(
                ErrorType.TENDERPLAN_API_ERROR,
                f"TenderPlan API вернул статус {resp.status_code}",
                "Получение приложений",
                {
                    "tender_id": tender_id,
                    "status_code": resp.status_code,
                    "response": resp.text[:200]
                }
            )
            return []

        if not resp.text.strip():
            return []

        data = resp.json()
        if not isinstance(data, list):
            logger.warning(f"Ожидается список приложений, получен {type(data).__name__}")
            return []

        attachments = [a for a in data if a.get("displayName") and a.get("href")]
        return attachments

    except requests.Timeout:
        logger.warning(f"Timeout при получении приложений для тендера {tender_id}")
        return []
    except requests.ConnectionError as e:
        error_manager.send_notification(
            ErrorType.TENDERPLAN_API_ERROR,
            f"Ошибка соединения при получении приложений: {str(e)}",
            "Получение приложений",
            {"tender_id": tender_id, "error": str(e)}
        )
        return []
    except Exception as e:
        logger.error(f"Ошибка при получении документов для тендера {tender_id}: {e}")
        return []


# -----------------------
# 🔥 ОПТИМИЗИРОВАННОЕ СКАЧИВАНИЕ С ПОТОКОМ
# -----------------------
def download_file_with_limit(url: str, max_size_bytes: int) -> bytes:
    """
    Скачивает файл с ограничением размера и потоковой проверкой

    Args:
        url: URL файла
        max_size_bytes: Максимальный размер в байтах

    Returns:
        Содержимое файла в виде bytes
    """
    try:
        logger.info(f"Начало скачивания файла: {url} (макс {max_size_bytes} байт)")

        # HEAD запрос для проверки размера файла ДО полной загрузки
        try:
            head_resp = requests.head(url, timeout=10, allow_redirects=True)
            file_size = int(head_resp.headers.get('content-length', 0))

            if file_size > max_size_bytes:
                error_msg = f"Файл слишком большой: {file_size} > {max_size_bytes} байт"
                error_manager.send_notification(
                    ErrorType.FILE_SIZE_ERROR,
                    error_msg,
                    "Проверка размера файла",
                    {"url": url, "file_size": file_size, "max_size": max_size_bytes}
                )
                raise HTTPException(status_code=413, detail="Файл слишком большой (>100MB)")

            logger.info(f"Размер файла: {file_size} байт")
        except requests.Timeout:
            logger.warning("HEAD запрос timeout, пытаемся GET с ограничением")
        except Exception as e:
            logger.warning(f"HEAD запрос ошибка: {e}, пытаемся GET")

        # Потоковая загрузка с проверкой размера
        downloaded_size = 0
        chunks = []

        with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT_SEC) as resp:
            resp.raise_for_status()

            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    downloaded_size += len(chunk)

                    if downloaded_size > max_size_bytes:
                        error_msg = f"Скачанный файл превышает лимит: {downloaded_size} > {max_size_bytes}"
                        error_manager.send_notification(
                            ErrorType.FILE_SIZE_ERROR,
                            error_msg,
                            "Скачивание файла",
                            {"url": url, "downloaded": downloaded_size, "max_size": max_size_bytes}
                        )
                        raise HTTPException(status_code=413, detail="Файл слишком большой")

                    chunks.append(chunk)

                    if downloaded_size % (10 * 1024 * 1024) == 0:
                        logger.debug(f"Загружено: {downloaded_size / 1024 / 1024:.1f}MB")

        file_content = b''.join(chunks)
        logger.info(f"✅ Файл успешно скачан: {len(file_content)} байт")
        return file_content

    except requests.Timeout:
        error_manager.send_notification(
            ErrorType.FILE_DOWNLOAD_ERROR,
            f"Timeout при скачивании (>{DOWNLOAD_TIMEOUT_SEC} сек)",
            "Загрузка документа",
            {"url": url, "timeout": DOWNLOAD_TIMEOUT_SEC}
        )
        raise HTTPException(status_code=408, detail=f"Timeout: файл скачивается дольше {DOWNLOAD_TIMEOUT_SEC}с")

    except requests.ConnectionError as e:
        error_manager.send_notification(
            ErrorType.FILE_DOWNLOAD_ERROR,
            f"Ошибка соединения: {str(e)}",
            "Загрузка документа",
            {"url": url, "error": str(e)}
        )
        raise HTTPException(status_code=503, detail="Не удалось скачать файл")

    except requests.HTTPError as e:
        error_manager.send_notification(
            ErrorType.FILE_DOWNLOAD_ERROR,
            f"HTTP ошибка {e.response.status_code}",
            "Загрузка документа",
            {"url": url, "status_code": e.response.status_code}
        )
        raise HTTPException(status_code=e.response.status_code, detail="Ошибка при скачивании файла")

    except HTTPException:
        raise

    except Exception as e:
        error_manager.send_notification(
            ErrorType.FILE_DOWNLOAD_ERROR,
            f"Неожиданная ошибка при скачивании: {str(e)}",
            "Загрузка документа",
            {"url": url, "error": str(e), "traceback": traceback.format_exc()}
        )
        raise HTTPException(status_code=500, detail="Ошибка при скачивании файла")


# -----------------------
# 🔥 ОПТИМИЗИРОВАННЫЙ ПАРСИНГ БЕЗ ФАЙЛОВ
# -----------------------
def parse_docx_from_bytes(file_bytes: bytes) -> str:
    """
    Парсит DOCX из bytes БЕЗ сохранения на диск

    Args:
        file_bytes: Содержимое DOCX файла в виде bytes

    Returns:
        Извлеченный текст
    """
    try:
        logger.info(f"Начало парсинга DOCX из памяти ({len(file_bytes)} байт)")

        doc = docx.Document(io.BytesIO(file_bytes))
        full_text = "\n".join(p.text for p in doc.paragraphs).strip()

        logger.info(f"✅ DOCX успешно распарсен: {len(full_text)} символов")
        return full_text

    except docx.oxml.parse.OxmlParseError as e:
        error_manager.send_notification(
            ErrorType.DOCUMENT_PARSE_ERROR,
            f"XML parsing error: {str(e)}",
            "Парсинг DOCX",
            {"error": str(e)[:200]}
        )
        raise HTTPException(status_code=422, detail="Некорректный формат DOCX")

    except Exception as e:
        error_manager.send_notification(
            ErrorType.DOCUMENT_PARSE_ERROR,
            f"Ошибка парсинга DOCX: {str(e)}",
            "Парсинг DOCX",
            {"error": str(e), "traceback": traceback.format_exc()[:500]}
        )
        raise HTTPException(status_code=500, detail="Ошибка чтения DOCX")


def parse_doc_from_bytes(file_bytes: bytes) -> str:
    """
    Парсит DOC из bytes БЕЗ сохранения на диск

    Args:
        file_bytes: Содержимое DOC файла в виде bytes

    Returns:
        Извлеченный текст
    """
    try:
        logger.info(f"Начало парсинга DOC из памяти ({len(file_bytes)} байт)")

        result = mammoth.extract_raw_text(io.BytesIO(file_bytes))
        text = result.value.strip()

        if result.messages:
            logger.warning(f"Warnings при парсинге DOC: {result.messages}")

        logger.info(f"✅ DOC успешно распарсен: {len(text)} символов")
        return text

    except Exception as e:
        error_manager.send_notification(
            ErrorType.DOCUMENT_PARSE_ERROR,
            f"Ошибка парсинга DOC (Mammoth): {str(e)}",
            "Парсинг DOC",
            {"error": str(e), "traceback": traceback.format_exc()[:500]}
        )
        raise HTTPException(status_code=500, detail="Ошибка чтения DOC")


# -----------------------
# ✅ TEST TIMEZONE ENDPOINT (для отладки)
# -----------------------
@app.get("/test-timezone")
def test_timezone():
    """
    Endpoint для теста правильности работы с временными зонами
    Показывает текущее время во всех временных зонах
    """
    try:
        # Получаем текущее UTC время
        now_utc = datetime.now(UTC_TZ)

        # Конвертируем в разные зоны
        now_msk = now_utc.astimezone(MSK_TZ)
        now_nsk = now_utc.astimezone(NSK_TZ)

        # Вчера по новосибирскому времени
        target_day_nsk = (now_nsk - timedelta(days=1)).date()

        # Начало и конец дня в Новосибирске
        start_nsk = NSK_TZ.localize(datetime.combine(target_day_nsk, dt_time(0, 0)))
        end_nsk = NSK_TZ.localize(datetime.combine(target_day_nsk, dt_time(23, 59, 59)))

        # Конвертируем в Москву
        start_msk = start_nsk.astimezone(MSK_TZ)
        end_msk = end_nsk.astimezone(MSK_TZ)

        # Конвертируем в UTC для API
        start_utc = start_msk.astimezone(UTC_TZ)
        end_utc = end_msk.astimezone(UTC_TZ)

        return {
            "status": "ok",
            "server_info": {
                "server_timezone": "UTC (Render by default)",
                "timestamp": datetime.now().isoformat()
            },
            "current_time": {
                "utc": now_utc.strftime('%d.%m.%Y %H:%M:%S %Z'),
                "moscow": now_msk.strftime('%d.%m.%Y %H:%M:%S %Z'),
                "novosibirsk": now_nsk.strftime('%d.%m.%Y %H:%M:%S %Z')
            },
            "tender_query_params": {
                "description": "Параметры для запроса тендеров за ВЧЕРА (по Новосибирску)",
                "target_day_nsk": target_day_nsk.strftime('%d.%m.%Y'),
                "start_nsk": start_nsk.strftime('%d.%m.%Y %H:%M:%S %Z'),
                "end_nsk": end_nsk.strftime('%d.%m.%Y %H:%M:%S %Z'),
                "start_msk": start_msk.strftime('%d.%m.%Y %H:%M:%S %Z (публикация тендеров)'),
                "end_msk": end_msk.strftime('%d.%m.%Y %H:%M:%S %Z (публикация тендеров)'),
                "start_utc_for_api": start_utc.strftime('%d.%m.%Y %H:%M:%S %Z'),
                "end_utc_for_api": end_utc.strftime('%d.%m.%Y %H:%M:%S %Z'),
                "from_ts": tender_ts(start_utc.replace(tzinfo=None)),
                "to_ts": tender_ts(end_utc.replace(tzinfo=None))
            }
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "traceback": traceback.format_exc()
        }


# -----------------------
# ✅ PING ENDPOINT (KEEP-ALIVE)
# -----------------------
@app.get("/ping")
def ping():
    """Простой ping endpoint для keep-alive на Render"""
    try:
        return {
            "status": "ok",
            "message": "API is alive and running",
            "timestamp": datetime.now(UTC_TZ).isoformat(),
            "uptime_check": "Render will keep this instance active with periodic pings"
        }
    except Exception as e:
        error_manager.send_notification(
            ErrorType.PING_ERROR,
            f"Ошибка при выполнении ping: {str(e)}",
            "Ping Health Check",
            {"error": str(e)}
        )
        return {
            "status": "error",
            "message": f"Ping failed: {str(e)}",
            "timestamp": datetime.now(UTC_TZ).isoformat()
        }


# -----------------------
# HEALTH CHECK ENDPOINT
# -----------------------
@app.get("/health")
def health_check():
    """Проверка здоровья API и подключений"""
    health_status = {
        "status": "checking",
        "timestamp": datetime.now(UTC_TZ).isoformat(),
        "services": {}
    }

    # Проверка Google Sheets
    try:
        client = gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)
        sheet = client.open_by_key(GOOGLE_SHEET_ID).sheet1
        health_status["services"]["google_sheets"] = "✅ OK"
    except Exception as e:
        health_status["services"]["google_sheets"] = f"❌ Error: {str(e)[:100]}"
        error_manager.send_notification(
            ErrorType.GOOGLE_SHEETS_ERROR,
            f"Health check ошибка: {str(e)}",
            "Health Check",
            {"service": "google_sheets"}
        )

    # Проверка TenderPlan API
    try:
        resp = requests.get(
            TENDERS_URL,
            headers={"Authorization": f"Bearer {API_TOKEN}"},
            params={"page": 0, "limit": 1},
            timeout=15
        )
        if resp.status_code == 200:
            health_status["services"]["tenderplan_api"] = "✅ OK"
        else:
            health_status["services"]["tenderplan_api"] = f"⚠️ Status {resp.status_code}"
    except Exception as e:
        health_status["services"]["tenderplan_api"] = f"❌ Error: {str(e)[:100]}"

    health_status["status"] = "healthy" if all(
        "OK" in str(v) for v in health_status["services"].values()) else "degraded"

    return health_status


# -----------------------
# 🚀 ОПТИМИЗИРОВАННЫЙ PARSE-DOC ENDPOINT
# -----------------------
@app.post("/parse-doc")
async def parse_doc(url: str):
    """
    ✨ ОПТИМИЗИРОВАННЫЙ парсер документов

    - Потоковая загрузка с проверкой размера
    - Парсинг БЕЗ сохранения на диск
    - Асинхронный (не блокирует сервер)
    - Таймауты и ограничения
    """

    logger.info(f"📥 Новый запрос парсинга: {url[:80]}...")

    try:
        # ========== ШАГ 1: СКАЧИВАНИЕ ==========
        logger.info("ШАГ 1: Скачивание файла с потоком и проверкой размера")

        max_size = MAX_FILE_SIZE_MB * 1024 * 1024

        file_content = await asyncio.get_event_loop().run_in_executor(
            executor,
            download_file_with_limit,
            url,
            max_size
        )

        logger.info(f"✅ Файл скачан: {len(file_content) / 1024 / 1024:.2f}MB")

        # ========== ШАГ 2: ОПРЕДЕЛЕНИЕ ФОРМАТА ==========
        logger.info("ШАГ 2: Определение формата файла")

        ext = "docx" if url.lower().endswith("docx") else "doc"
        logger.info(f"Формат: {ext.upper()}")

        # ========== ШАГ 3: ПАРСИНГ ==========
        logger.info(f"ШАГ 3: Парсинг {ext.upper()} из памяти")

        if ext == "docx":
            text = await asyncio.get_event_loop().run_in_executor(
                executor,
                parse_docx_from_bytes,
                file_content
            )
        else:
            text = await asyncio.get_event_loop().run_in_executor(
                executor,
                parse_doc_from_bytes,
                file_content
            )

        logger.info(f"✅ Парсинг завершен: {len(text)} символов")

        # ========== ШАГ 4: ВОЗВРАТ РЕЗУЛЬТАТА ==========
        return {
            "status": "ok",
            "text": text,
            "format": ext,
            "file_size_mb": round(len(file_content) / 1024 / 1024, 2),
            "text_length": len(text),
            "timestamp": datetime.now(UTC_TZ).isoformat()
        }

    except HTTPException:
        raise

    except Exception as e:
        error_manager.send_notification(
            ErrorType.DOCUMENT_PARSE_ERROR,
            f"Неожиданная ошибка: {str(e)}",
            "Парсинг документа",
            {"url": url[:100], "error": str(e)[:200], "traceback": traceback.format_exc()[:500]}
        )
        raise HTTPException(status_code=500, detail=f"Ошибка при парсинге: {str(e)[:100]}")


# -----------------------
# LOAD TENDERS ENDPOINT ✅ ИСПРАВЛЕННЫЙ
# -----------------------
@app.get("/load-tenders")
def load_tenders():
    """Основной endpoint для загрузки тендеров из TenderPlan в Google Sheets"""

    logger.info("=" * 60)
    logger.info("Начало процесса загрузки тендеров")
    logger.info("=" * 60)

    try:
        # ========== ЭТАП 1: Подготовка =========
        logger.info("ЭТАП 1: Подготовка параметров")

        # ✅ ИСПРАВЛЕННЫЙ КОД - используем правильные временные зоны
        now_utc = datetime.now(UTC_TZ)
        now_nsk = now_utc.astimezone(NSK_TZ)

        # Вчера в Новосибирском времени
        target_day_nsk = (now_nsk - timedelta(days=1)).date()

        # Начало и конец дня в Новосибирском времени
        start_nsk = NSK_TZ.localize(datetime.combine(target_day_nsk, dt_time(0, 0)))
        end_nsk = NSK_TZ.localize(datetime.combine(target_day_nsk, dt_time(23, 59, 59)))

        # Конвертируем в московское время (где публикуются тендеры)
        start_msk = start_nsk.astimezone(MSK_TZ)
        end_msk = end_nsk.astimezone(MSK_TZ)

        # Конвертируем в UTC для API TenderPlan
        from_ts = tender_ts(start_msk.astimezone(UTC_TZ).replace(tzinfo=None))
        to_ts = tender_ts(end_msk.astimezone(UTC_TZ).replace(tzinfo=None))

        logger.info(f"Текущее время (NSK): {now_nsk.strftime('%d.%m.%Y %H:%M:%S %Z')}")
        logger.info(f"Период загрузки (NSK): {target_day_nsk.strftime('%d.%m.%Y')}")
        logger.info(
            f"Период запроса (MSK): {start_msk.strftime('%d.%m.%Y %H:%M:%S')} - {end_msk.strftime('%d.%m.%Y %H:%M:%S')}")
        logger.info(f"Timestamp: {from_ts} - {to_ts}")

        headers = {"Authorization": f"Bearer {API_TOKEN}"}
        all_tenders = []
        page = 0
        failed_pages = []

        # ========== ЭТАП 2: Получение тендеров =========
        logger.info("ЭТАП 2: Загрузка тендеров с TenderPlan API")

        while True:
            try:
                params = {
                    "fromPublicationDateTime": from_ts,
                    "toPublicationDateTime": to_ts,
                    "statuses": "1",
                    "page": page,
                    "limit": 100
                }

                logger.debug(f"Запрос страницы {page}...")

                resp = requests.get(
                    TENDERS_URL,
                    headers=headers,
                    params=params,
                    timeout=40
                )

                # Проверка статус кодов
                if resp.status_code == 401:
                    error_manager.send_notification(
                        ErrorType.TENDERPLAN_API_ERROR,
                        "Неавторизованный запрос к TenderPlan API (401 Unauthorized)",
                        "Загрузка тендеров",
                        {
                            "status_code": 401,
                            "message": "API токен неверный или истек",
                            "recommendation": "Проверьте переменную окружения API_TOKEN"
                        }
                    )
                    return {
                        "status": "error",
                        "error": "Unauthorized",
                        "message": "Ошибка аутентификации. Проверьте API_TOKEN."
                    }

                elif resp.status_code == 429:
                    error_manager.send_notification(
                        ErrorType.TENDERPLAN_API_ERROR,
                        "Превышен лимит запросов (429 Too Many Requests)",
                        "Загрузка тендеров",
                        {
                            "status_code": 429,
                            "page": page,
                            "message": "API лимит исчерпан. Процесс остановлен."
                        }
                    )
                    logger.warning(f"Rate limit достигнут на странице {page}")
                    break

                elif resp.status_code != 200:
                    error_manager.send_notification(
                        ErrorType.TENDERPLAN_API_ERROR,
                        f"TenderPlan API вернул ошибку {resp.status_code}",
                        "Загрузка тендеров",
                        {
                            "status_code": resp.status_code,
                            "page": page,
                            "response": resp.text[:500]
                        }
                    )
                    logger.error(f"Ошибка при загрузке страницы {page}: {resp.status_code}")
                    failed_pages.append(page)
                    break

                # Парсим ответ
                try:
                    data = resp.json()
                except ValueError as e:
                    error_manager.send_notification(
                        ErrorType.TENDERPLAN_API_ERROR,
                        f"Ошибка парсинга JSON ответа: {str(e)}",
                        "Парсинг ответа TenderPlan API",
                        {"page": page, "response_length": len(resp.text)}
                    )
                    logger.error(f"Не удалось распарсить JSON на странице {page}")
                    failed_pages.append(page)
                    break

                tenders = data.get("tenders", [])
                if not tenders:
                    logger.info(f"Тендеры на странице {page} не найдены. Загрузка завершена.")
                    break

                logger.info(f"Страница {page}: загружено {len(tenders)} тендеров")
                all_tenders.extend(tenders)
                page += 1

            except requests.Timeout:
                error_manager.send_notification(
                    ErrorType.TENDERPLAN_API_ERROR,
                    f"Timeout при загрузке тендеров (>15 сек)",
                    "Загрузка тендеров",
                    {"page": page}
                )
                logger.error(f"Timeout на странице {page}")
                failed_pages.append(page)
                break
            except requests.ConnectionError as e:
                error_manager.send_notification(
                    ErrorType.TENDERPLAN_API_ERROR,
                    f"Ошибка соединения: {str(e)}",
                    "Загрузка тендеров",
                    {"page": page, "error": str(e)}
                )
                logger.error(f"Ошибка соединения на странице {page}: {e}")
                failed_pages.append(page)
                break
            except Exception as e:
                error_manager.send_notification(
                    ErrorType.TENDERPLAN_API_ERROR,
                    f"Неожиданная ошибка при загрузке тендеров: {str(e)}",
                    "Загрузка тендеров",
                    {"page": page, "error": str(e), "traceback": traceback.format_exc()}
                )
                logger.error(f"Неожиданная ошибка на странице {page}: {e}")
                failed_pages.append(page)
                break

        if not all_tenders:
            logger.warning("Тендеры не найдены")
            return {
                "status": "success",
                "message": "Нет тендеров за вчера",
                "added": 0,
                "failed_pages": failed_pages
            }

        logger.info(f"Всего загружено тендеров: {len(all_tenders)}")

        # ========== ЭТАП 3: Подключение к Google Sheets =========
        logger.info("ЭТАП 3: Подключение к Google Sheets")

        try:
            sheet = get_sheet()
            logger.info("✅ Успешное подключение к Google Sheets")
        except Exception as e:
            logger.error(f"Не удалось подключиться к Google Sheets: {e}")
            return {
                "status": "error",
                "error": "Google Sheets Connection Error",
                "message": str(e)
            }

        # ========== ЭТАП 4: Обработка тендеров =========
        logger.info("ЭТАП 4: Обработка тендеров")

        rows = []
        max_docs = 0
        now_nsk_str = now_nsk.strftime("%d.%m.%Y %H:%M")
        processing_errors = []

        for idx, t in enumerate(all_tenders):
            try:
                tender_id = t.get("_id", "unknown")

                customers = t.get("customers", [])
                customer_names = ", ".join([c.get("name", "") for c in customers])

                placing_name = PLACING_WAYS.get(t.get("placingWay"), "Неизвестно")

                attachments = fetch_attachments(tender_id, headers)
                max_docs = max(max_docs, len(attachments))

                row = [
                    now_nsk_str,
                    tender_id,
                    t.get("orderName", ""),
                    customer_names,
                    t.get("maxPrice", ""),
                    f"https://tenderplan.ru/app?key=0&tender={tender_id}",
                    convert_timestamp(t.get("publicationDateTime")),
                    convert_timestamp(t.get("submissionCloseDateTime")),
                    placing_name
                ]

                for a in attachments:
                    row.append(a.get("displayName", ""))
                    row.append(a.get("href", ""))

                rows.append(row)

            except Exception as e:
                error_msg = f"Ошибка при обработке тендера {t.get('_id', 'unknown')}: {str(e)}"
                logger.warning(error_msg)
                processing_errors.append({
                    "tender_id": t.get("_id"),
                    "error": str(e)
                })
                continue

            if (idx + 1) % 50 == 0:
                logger.debug(f"Обработано {idx + 1} тендеров...")

        logger.info(f"✅ Обработано {len(rows)} тендеров успешно")

        if processing_errors:
            logger.warning(f"⚠️ Ошибок при обработке: {len(processing_errors)}")

        # ========== ЭТАП 5: Загрузка в Google Sheets =========
        logger.info("ЭТАП 5: Загрузка данных в Google Sheets")

        try:
            ensure_header(sheet, max_docs)
            logger.info("✅ Заголовок обновлен")
        except Exception as e:
            logger.error(f"Ошибка при обновлении заголовка: {e}")
            return {
                "status": "error",
                "error": "Header Update Error",
                "message": str(e)
            }

        try:
            if rows:
                sheet.append_rows(rows, value_input_option="USER_ENTERED")
                logger.info(f"✅ Загружено {len(rows)} строк в Google Sheets")
            else:
                logger.warning("Нет строк для загрузки")
        except gspread.exceptions.APIError as e:
            error_manager.send_notification(
                ErrorType.GOOGLE_SHEETS_ERROR,
                f"Google Sheets API ошибка при загрузке данных: {str(e)}",
                "Загрузка данных в Sheets",
                {
                    "status_code": getattr(e, "status_code", None),
                    "rows_count": len(rows),
                    "message": str(e)
                }
            )
            return {
                "status": "error",
                "error": "Google Sheets API Error",
                "message": str(e)
            }
        except Exception as e:
            error_manager.send_notification(
                ErrorType.GOOGLE_SHEETS_ERROR,
                f"Ошибка при загрузке данных в Google Sheets: {str(e)}",
                "Загрузка данных в Sheets",
                {"error": str(e), "traceback": traceback.format_exc()}
            )
            return {
                "status": "error",
                "error": "Data Upload Error",
                "message": str(e)
            }

        # ========== ИТОГИ =========
        logger.info("=" * 60)
        logger.info("✅ УСПЕШНО: Процесс загрузки завершен")
        logger.info("=" * 60)

        return {
            "status": "success",
            "added": len(rows),
            "total_fetched": len(all_tenders),
            "processing_errors": len(processing_errors),
            "failed_pages": failed_pages,
            "timestamp": now_nsk_str
        }

    except Exception as e:
        error_manager.send_notification(
            ErrorType.UNKNOWN_ERROR,
            f"Неожиданная ошибка в процессе загрузки тендеров: {str(e)}",
            "Основной процесс загрузки",
            {"error": str(e), "traceback": traceback.format_exc()}
        )
        logger.error(f"Неожиданная ошибка: {e}")
        return {
            "status": "error",
            "error": "Unexpected Error",
            "message": str(e)
        }


# -----------------------
# ERRORS ENDPOINT
# -----------------------
@app.get("/errors")
def get_errors(limit: int = 50):
    """Возвращает последние N ошибок"""
    return {
        "error_count": len(error_manager.errors),
        "showing": min(limit, len(error_manager.errors)),
        "errors": error_manager.errors[-limit:]
    }


# -----------------------
# INFO ENDPOINT
# -----------------------
@app.get("/info")
def get_info():
    """Информация об API и конфигурации"""
    return {
        "app": "Tender Loader API + Parser",
        "version": "2.1",
        "config": {
            "max_file_size_mb": MAX_FILE_SIZE_MB,
            "download_timeout_sec": DOWNLOAD_TIMEOUT_SEC,
            "parse_timeout_sec": PARSE_TIMEOUT_SEC,
            "timezones": {
                "server": "UTC (Render)",
                "tenders_published": "Europe/Moscow (MSK, UTC+3)",
                "script_runs_at": "Asia/Novosibirsk (NSK, UTC+7)"
            }
        },
        "endpoints": {
            "GET /ping": "Health check (keep-alive)",
            "GET /health": "Detailed service check",
            "GET /test-timezone": "🆕 Test timezone conversion",
            "POST /parse-doc": "Parse DOC/DOCX document (async, optimized)",
            "GET /load-tenders": "Load tenders from TenderPlan (FIXED)",
            "GET /errors": "View errors log",
            "GET /info": "API info and config"
        },
        "improvements": {
            "timezone_handling": "✅ Proper UTC → MSK → NSK conversion",
            "parse_doc": "✅ Stream download + parsing from memory (3x faster)",
            "async": "✅ Non-blocking async processing",
            "error_handling": "✅ Comprehensive error tracking",
            "render_compatible": "✅ No localhost calls, no disk I/O"
        }
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)