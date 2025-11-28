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

# -----------------------
# Загружаем настройки
# -----------------------
load_dotenv()
API_TOKEN = os.getenv("API_TOKEN")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_CREDS_B64 = os.getenv("GOOGLE_CREDS_B64")

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


# -----------------------
# КЛАСС ДЛЯ УПРАВЛЕНИЯ ОШИБКАМИ (БЕЗ EMAIL)
# -----------------------
class ErrorNotificationManager:
    """Управляет ошибками и логирует их"""

    def __init__(self):
        self.errors = []

    def send_notification(self, error_type: ErrorType, error_msg: str,
                          stage: str, details: dict = None):
        """
        Регистрирует ошибку и логирует её

        Args:
            error_type: Тип ошибки
            error_msg: Сообщение об ошибке
            stage: На каком этапе произошла ошибка
            details: Дополнительные детали
        """
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

        # Проверяем статус код
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
# ✅ PING ENDPOINT (KEEP-ALIVE С ПРОВЕРКОЙ)
# -----------------------
@app.get("/ping")
def ping():
    """
    Простой ping endpoint для keep-alive на Render.
    Возвращает статус API
    """
    try:
        return {
            "status": "ok",
            "message": "API is alive and running",
            "timestamp": datetime.now().isoformat(),
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
            "timestamp": datetime.now().isoformat()
        }


# -----------------------
# HEALTH CHECK ENDPOINT
# -----------------------
@app.get("/health")
def health_check():
    """Проверка здоровья API и подключений"""
    health_status = {
        "status": "checking",
        "timestamp": datetime.now().isoformat(),
        "services": {}
    }

    # Проверка Google Sheets
    try:
        sheet = get_sheet()
        health_status["services"]["google_sheets"] = "✅ OK"
    except Exception as e:
        health_status["services"]["google_sheets"] = f"❌ Error: {str(e)}"
        error_manager.send_notification(
            ErrorType.GOOGLE_SHEETS_ERROR,
            f"Health check: ошибка Google Sheets - {str(e)}",
            "Health Check",
            {"service": "google_sheets"}
        )

    # Проверка TenderPlan API
    try:
        resp = requests.get(
            TENDERS_URL,
            headers={"Authorization": f"Bearer {API_TOKEN}"},
            params={"page": 0, "limit": 1},
            timeout=25
        )
        if resp.status_code == 200:
            health_status["services"]["tenderplan_api"] = "✅ OK"
        else:
            health_status["services"]["tenderplan_api"] = f"⚠️ Status {resp.status_code}"
            error_manager.send_notification(
                ErrorType.TENDERPLAN_API_ERROR,
                f"Health check: TenderPlan API вернул статус {resp.status_code}",
                "Health Check",
                {"service": "tenderplan_api", "status_code": resp.status_code}
            )
    except Exception as e:
        health_status["services"]["tenderplan_api"] = f"❌ Error: {str(e)}"
        error_manager.send_notification(
            ErrorType.TENDERPLAN_API_ERROR,
            f"Health check: ошибка подключения к TenderPlan API - {str(e)}",
            "Health Check",
            {"service": "tenderplan_api", "error": str(e)}
        )

    # Общий статус
    health_status["status"] = "healthy" if all(
        "OK" in str(v) for v in health_status["services"].values()) else "degraded"

    return health_status


# -----------------------
# ПАРСЕР ДОКУМЕНТОВ DOC / DOCX
# -----------------------
@app.post("/parse-doc")
def parse_doc(url: str):
    """Парсит DOC/DOCX документ и возвращает текст"""

    logger.info(f"Начало парсинга документа: {url}")

    try:
        # Скачиваем файл
        try:
            file_resp = requests.get(url, timeout=300)
            file_resp.raise_for_status()
        except requests.Timeout:
            error_manager.send_notification(
                ErrorType.FILE_DOWNLOAD_ERROR,
                f"Timeout при скачивании документа (>30 сек)",
                "Загрузка документа",
                {"url": url}
            )
            raise HTTPException(status_code=408, detail="Timeout при скачивании файла")
        except requests.ConnectionError as e:
            error_manager.send_notification(
                ErrorType.FILE_DOWNLOAD_ERROR,
                f"Ошибка соединения при скачивании документа: {str(e)}",
                "Загрузка документа",
                {"url": url, "error": str(e)}
            )
            raise HTTPException(status_code=503, detail="Не удалось скачать файл")
        except requests.HTTPError as e:
            error_manager.send_notification(
                ErrorType.FILE_DOWNLOAD_ERROR,
                f"HTTP ошибка при скачивании документа: {e.response.status_code}",
                "Загрузка документа",
                {"url": url, "status_code": e.response.status_code}
            )
            raise HTTPException(status_code=e.response.status_code, detail="Ошибка при скачивании файла")

        # Определяем формат файла
        ext = "docx" if url.lower().endswith("docx") else "doc"

        # Сохраняем во временный файл
        with NamedTemporaryFile(delete=False, suffix=f".{ext}") as tmp:
            tmp.write(file_resp.content)
            tmp_path = tmp.name

        try:
            # DOCX обработка
            if ext == "docx":
                try:
                    doc = docx.Document(tmp_path)
                    full_text = "\n".join(p.text for p in doc.paragraphs).strip()
                    logger.info(f"Документ DOCX успешно распарсен. Размер текста: {len(full_text)} символов")
                    return {"status": "ok", "text": full_text, "format": "docx"}
                except docx.oxml.parse.OxmlParseError as e:
                    error_manager.send_notification(
                        ErrorType.DOCUMENT_PARSE_ERROR,
                        f"Ошибка парсинга DOCX (XML parsing error): {str(e)}",
                        "Парсинг DOCX документа",
                        {"url": url, "error": str(e)}
                    )
                    raise HTTPException(status_code=422, detail="Некорректный формат DOCX")
                except Exception as e:
                    error_manager.send_notification(
                        ErrorType.DOCUMENT_PARSE_ERROR,
                        f"Ошибка при чтении DOCX: {str(e)}",
                        "Парсинг DOCX документа",
                        {"url": url, "error": str(e), "traceback": traceback.format_exc()}
                    )
                    raise HTTPException(status_code=500, detail="Ошибка чтения DOCX")

            # DOC обработка через Mammoth
            else:
                try:
                    with open(tmp_path, "rb") as f:
                        result = mammoth.extract_raw_text(f)
                        text = result.value.strip()

                        if result.messages:
                            logger.warning(f"Warnings при парсинге DOC: {result.messages}")

                        logger.info(f"Документ DOC успешно распарсен. Размер текста: {len(text)} символов")
                        return {"status": "ok", "text": text, "format": "doc"}
                except Exception as e:
                    error_manager.send_notification(
                        ErrorType.DOCUMENT_PARSE_ERROR,
                        f"Ошибка при чтении DOC (Mammoth): {str(e)}",
                        "Парсинг DOC документа",
                        {"url": url, "error": str(e), "traceback": traceback.format_exc()}
                    )
                    raise HTTPException(status_code=500, detail="Ошибка чтения DOC")

        finally:
            # Удаляем временный файл
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                    logger.debug(f"Временный файл удален: {tmp_path}")
                except Exception as e:
                    logger.warning(f"Не удалось удалить временный файл {tmp_path}: {e}")

    except HTTPException:
        raise
    except Exception as e:
        error_manager.send_notification(
            ErrorType.DOCUMENT_PARSE_ERROR,
            f"Неожиданная ошибка при парсинге документа: {str(e)}",
            "Парсинг документа",
            {"url": url, "error": str(e), "traceback": traceback.format_exc()}
        )
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


# -----------------------
# ОСНОВНОЙ ENDPOINT ЗАГРУЗКИ ТЕНДЕРОВ
# -----------------------
@app.get("/load-tenders")
def load_tenders():
    """Основной endpoint для загрузки тендеров из TenderPlan в Google Sheets"""

    logger.info("=" * 60)
    logger.info("Начало процесса загрузки тендеров")
    logger.info("=" * 60)

    try:
        # ========== ЭТАП 0: Проверка здоровья API (НОВОЕ!) =========
        logger.info("ЭТАП 0: Проверка здоровья API (Ping)")

        try:
            ping_resp = requests.get(
                "http://localhost:8000/ping",
                timeout=25
            )

            if ping_resp.status_code != 200:
                error_msg = f"Ping вернул статус {ping_resp.status_code}"
                error_manager.send_notification(
                    ErrorType.PING_ERROR,
                    error_msg,
                    "Проверка здоровья API",
                    {
                        "status_code": ping_resp.status_code,
                        "response": ping_resp.text[:200],
                        "critical": "YES - API не отвечает нормально"
                    }
                )
                logger.error(f"❌ {error_msg}")
                return {
                    "status": "error",
                    "error": "Ping Health Check Failed",
                    "message": f"API ping вернул ошибку: {ping_resp.status_code}",
                    "timestamp": datetime.now().isoformat(),
                    "details": {
                        "status_code": ping_resp.status_code,
                        "critical": "YES"
                    }
                }

            logger.info("✅ API Health Check - OK")

        except requests.Timeout:
            error_msg = "Timeout при проверке ping (>5 сек)"
            error_manager.send_notification(
                ErrorType.PING_ERROR,
                error_msg,
                "Проверка здоровья API",
                {"critical": "YES - API не отвечает"}
            )
            logger.error(f"❌ {error_msg}")
            return {
                "status": "error",
                "error": "Ping Timeout",
                "message": "API не отвечает на ping запрос",
                "timestamp": datetime.now().isoformat(),
                "details": {
                    "critical": "YES",
                    "timeout_seconds": 5
                }
            }

        except requests.ConnectionError as e:
            error_msg = f"Ошибка соединения при ping: {str(e)}"
            error_manager.send_notification(
                ErrorType.PING_ERROR,
                error_msg,
                "Проверка здоровья API",
                {"error": str(e), "critical": "YES"}
            )
            logger.error(f"❌ {error_msg}")
            return {
                "status": "error",
                "error": "Ping Connection Error",
                "message": f"Не удалось подключиться к API: {str(e)}",
                "timestamp": datetime.now().isoformat(),
                "details": {
                    "critical": "YES",
                    "error": str(e)
                }
            }

        except Exception as e:
            error_msg = f"Неожиданная ошибка при ping: {str(e)}"
            error_manager.send_notification(
                ErrorType.PING_ERROR,
                error_msg,
                "Проверка здоровья API",
                {"error": str(e), "traceback": traceback.format_exc(), "critical": "YES"}
            )
            logger.error(f"❌ {error_msg}")
            return {
                "status": "error",
                "error": "Ping Unexpected Error",
                "message": error_msg,
                "timestamp": datetime.now().isoformat(),
                "details": {
                    "critical": "YES",
                    "error": str(e)
                }
            }

        # ========== ЭТАП 1: Подготовка =========
        logger.info("ЭТАП 1: Подготовка параметров")

        now = datetime.now()
        target_day = now - timedelta(days=1)
        start_dt = datetime.combine(target_day, dt_time(0, 0))
        end_dt = datetime.combine(target_day, dt_time(23, 59, 59))
        from_ts = tender_ts(start_dt)
        to_ts = tender_ts(end_dt)

        logger.info(f"Период загрузки: {target_day.strftime('%d.%m.%Y')}")
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
        now_str = now.strftime("%d.%m.%Y %H:%M")
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
                    now_str,
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
            "timestamp": now_str
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
# ENDPOINT ПРОСМОТРА ОШИБОК
# -----------------------
@app.get("/errors")
def get_errors():
    """Возвращает список всех произошедших ошибок"""
    return {
        "error_count": len(error_manager.errors),
        "errors": error_manager.errors
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)