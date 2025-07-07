import datetime
import time
import asyncio
import aiohttp
import logging
import sys
import string
from typing import List, Dict, Any, Optional, Tuple
import os
from supabase import create_client, Client
from telethon import TelegramClient
from decimal import Decimal, InvalidOperation

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# --- Configuration ---
API_URL: Optional[str] = os.environ.get("API_URL")
DEFAULT_HEADERS: Dict[str, str] = {
    "User-Agent": str(os.environ.get("USER_AGENT", "Mozilla/5.0")),
    "X-Requested-With": str(os.environ.get("X_REQUESTED_WITH", "XMLHttpRequest")),
    "Referer": str(os.environ.get("REFERER", "https://yourapp.com"))
}
REQUEST_TIMEOUT_SECONDS: int = int(os.environ.get("REQUEST_TIMEOUT_SECONDS", 30))
MAX_RETRIES: int = int(os.environ.get("MAX_RETRIES", 5))
RETRY_DELAY_SECONDS: int = int(os.environ.get("RETRY_DELAY_SECONDS", 2))
MAX_CONCURRENT_REQUESTS: int = int(os.environ.get("MAX_CONCURRENT_REQUESTS", 10))

SUPABASE_URL: Optional[str] = os.environ.get("SUPABASE_URL")
SUPABASE_KEY: Optional[str] = os.environ.get("SUPABASE_KEY")

# --- Logging Setup ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    fh = logging.FileHandler("connection_scraper.log", encoding='utf-8')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

# --- Supabase Client Init ---
supabase: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase client initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize Supabase client: {e}")
else:
    logger.error("SUPABASE_URL or SUPABASE_KEY not found. Supabase functionality will be disabled.")

# --- Helper Functions ---
def to_decimal_or_none(value: Any) -> Optional[Decimal]:
    if value is None: return None
    try: return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError): return None

def to_float_or_none(value: Any) -> Optional[float]:
    dec_val = to_decimal_or_none(value)
    return float(dec_val) if dec_val is not None else None

def safe_convert_timestamp(ts_str: Optional[str]) -> Optional[str]:
    if not ts_str or not str(ts_str).isdigit() or int(ts_str) == 0: return None
    try:
        ts_float = float(ts_str) / 1000.0
        dt_object = datetime.datetime.fromtimestamp(ts_float, tz=datetime.timezone.utc)
        return dt_object.isoformat()
    except (ValueError, TypeError, OSError): return None

# --- Data Mapping ---
def map_api_record_to_internal(api_record: dict) -> Optional[Dict[str, Any]]:
    if not api_record or not api_record.get('id'): return None
    return {
        'ID': api_record.get('id'),
        'Commercial Name (English)': api_record.get('name'),
        'Commercial Name (Arabic)': api_record.get('arabic'),
        'Scientific Name/Active Ingredients': api_record.get('active'),
        'Manufacturer': api_record.get('company'),
        'Current Price': to_float_or_none(api_record.get('price')),
        'Last Price Update Date': safe_convert_timestamp(api_record.get('Date_updated')),
        'Units': api_record.get('units'),
        'Barcode': api_record.get('barcode'),
        'Dosage Form': api_record.get('dosage_form'),
        'Uses (Arabic)': api_record.get('uses'),
        'Image URL': api_record.get('img'),
    }

# --- API Fetching Logic ---
async def fetch_drug_data_for_query(session: aiohttp.ClientSession, search_query: str, semaphore: asyncio.Semaphore) -> Tuple[str, List[Dict[str, Any]]]:
    payload = {"search": "1", "searchq": search_query, "order_by": "name ASC", "page": "1"}
    for attempt in range(MAX_RETRIES):
        try:
            async with semaphore:
                async with session.post(API_URL, data=payload, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                    response.raise_for_status()
                    data = await response.json(content_type=None)
                    return search_query, data.get('data', [])
        except Exception as e:
            wait_time = RETRY_DELAY_SECONDS * (2 ** attempt)
            logger.warning(f"API '{search_query}': Request failed ({type(e).__name__}) on attempt {attempt+1}/{MAX_RETRIES}. Retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)
    logger.error(f"API '{search_query}': All {MAX_RETRIES} retries failed.")
    return search_query, []

# --- NEW Text Notification Logic ---
def format_text_notification(change_info: Dict[str, Any]) -> str:
    """Formats the data into a clean, emoji-rich text message for Telegram."""
    curr = change_info['current']
    prev = change_info['previous']
    
    name_ar = curr.get('Commercial Name (Arabic)', "N/A")
    name_en = curr.get('Commercial Name (English)', "N/A")
    barcode = curr.get('Barcode', "N/A")
    new_price = to_decimal_or_none(curr.get('Current Price'))
    old_price = to_decimal_or_none(prev.get('current_price'))

    price_change_emoji = ""
    percent_emoji = ""
    percent_str = "N/A"

    if new_price is not None and old_price is not None and old_price > 0:
        if new_price > old_price:
            price_change_emoji = "⬆️"
            percent_emoji = "🟢"
        elif new_price < old_price:
            price_change_emoji = "⬇️"
            percent_emoji = "🔴"
        
        try:
            percent = ((new_price - old_price) / old_price) * 100
            percent_str = f"{percent:+.2f}%"
        except (InvalidOperation, TypeError):
            percent_str = "N/A"

    new_price_str = f"{new_price:g}" if new_price is not None else "N/A"
    old_price_str = f"{old_price:g}" if old_price is not None else "N/A"

    cairo_tz = datetime.timezone(datetime.timedelta(hours=3))
    timestamp = datetime.datetime.now(cairo_tz).strftime('%Y-%m-%d — %I:%M %p')

    # Using HTML for formatting in Telegram
    message = (
        f"<b>{name_ar}</b> 💊\n"
        f"<i>{name_en}</i>\n"
        f"-----------------------------------\n"
        f"<b>السعر الجديد: {new_price_str} ج.م</b> {price_change_emoji}\n"
        f"السعر السابق: {old_price_str} ج.م\n"
        f"نسبة التغيير: {percent_str} {percent_emoji}\n"
        f"-----------------------------------\n"
        f"الباركود: <code>{barcode}</code>\n"
        f"آخر تحديث: {timestamp}"
    )
    return message

async def send_telegram_message(message: str, client: TelegramClient) -> bool:
    """Sends a formatted text message to the target Telegram channel."""
    target_channel_str = os.environ.get("TARGET_CHANNEL")
    if not target_channel_str:
        logger.warning("TARGET_CHANNEL not set. Cannot send message.")
        return False
    try:
        target_channel = int(target_channel_str) if target_channel_str.lstrip('-').isdigit() else target_channel_str
        await client.send_message(target_channel, message, parse_mode='html')
        logger.info(f"Text notification sent successfully to channel {target_channel}.")
        return True
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}", exc_info=True)
        return False

# --- Main Logic (RPC-based) ---
async def process_and_commit_changes(drugs: List[Dict[str, Any]], telegram_client: Optional[TelegramClient]):
    if not supabase: logger.warning("Supabase client not initialized."); return
    if not drugs: return

    logger.info(f"Starting processing for {len(drugs)} unique drugs using RPC.")

    try:
        drugs_for_rpc = [
            {
                "id": str(drug.get("ID")),
                "commercial_name_en": drug.get("Commercial Name (English)"),
                "commercial_name_ar": drug.get("Commercial Name (Arabic)"),
                "active_ingredients": drug.get("Scientific Name/Active Ingredients"),
                "manufacturer": drug.get("Manufacturer"),
                "current_price": to_float_or_none(drug.get("Current Price")),
                "units": drug.get("Units"),
                "barcode": drug.get("Barcode"),
                "dosage_form": drug.get("Dosage Form"),
                "uses_ar": drug.get("Uses (Arabic)"),
                "image_url": drug.get("Image URL"),
                "last_price_update_date": drug.get("Last Price Update Date"),
            }
            for drug in drugs
        ]
        
        logger.info(f"Calling 'find_changed_drugs' RPC with {len(drugs_for_rpc)} records...")
        rpc_response = await asyncio.to_thread(
            lambda: supabase.rpc("find_changed_drugs", {"p_drugs": drugs_for_rpc}).execute()
        )
        changed_drugs = rpc_response.data
        logger.info(f"RPC call complete. Found {len(changed_drugs)} changed or new drugs.")

        if not changed_drugs:
            logger.info("No changes detected by the database.")
            return

        records_to_commit_to_db = []
        for change in changed_drugs:
            if change['change_type'] == 'NEW':
                records_to_commit_to_db.append(change)
                continue

            # This is a price update, send a text notification
            logger.info(f"Price change detected for ID {change['id']}: {change['previous_price']} -> {change['current_price']}")
            
            # Prepare data for the text formatter
            notification_data = {
                'current': {
                    'Commercial Name (Arabic)': change.get('commercial_name_ar'),
                    'Commercial Name (English)': change.get('commercial_name_en'),
                    'Barcode': change.get('barcode'),
                    'Current Price': change.get('current_price')
                },
                'previous': {'current_price': change.get('previous_price')}
            }

            notification_sent = False
            if telegram_client and telegram_client.is_connected():
                try:
                    message_text = format_text_notification(notification_data)
                    notification_sent = await send_telegram_message(message_text, telegram_client)
                except Exception as e:
                    logger.error(f"Error formatting or sending text notification for ID {change['id']}: {e}")

            if notification_sent:
                records_to_commit_to_db.append(change)
            else:
                logger.warning(f"Notification FAILED for ID {change['id']}. Skipping DB update for this run.")

        if not records_to_commit_to_db: return

        db_payload = [
            {
                "id": record['id'],
                "commercial_name_en": record['commercial_name_en'],
                "commercial_name_ar": record['commercial_name_ar'],
                "active_ingredients": record.get("active_ingredients"),
                "manufacturer": record.get("manufacturer"),
                "current_price": record['current_price'],
                "previous_price": record.get('previous_price'),
                "last_price_update_date": record.get("last_price_update_date"),
                "units": record.get("units"),
                "barcode": record.get("barcode"),
                "dosage_form": record.get("dosage_form"),
                "uses_ar": record.get("uses_ar"),
                "image_url": record.get("image_url"),
                "scraped_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
            for record in records_to_commit_to_db
        ]

        logger.info(f"Committing {len(db_payload)} records to the database...")
        await asyncio.to_thread(lambda: supabase.table("drugs").upsert(db_payload).execute())
        await asyncio.to_thread(lambda: supabase.table("history").insert(db_payload).execute())
        logger.info("Database commit successful.")

    except Exception as e:
        logger.exception(f"An unhandled error occurred during RPC-based processing: {e}")

# --- Main Execution ---
async def main():
    script_start_time = time.monotonic()
    logger.info(f"Script starting at {datetime.datetime.now(datetime.timezone.utc).isoformat()}...")

    telegram_client_instance: Optional[TelegramClient] = None
    api_id_str, api_hash, bot_token = os.environ.get("API_ID"), os.environ.get("API_HASH"), os.environ.get("BOT_TOKEN")
    if all([api_id_str, api_hash, bot_token]):
        try:
            api_id = int(api_id_str)
            telegram_client_instance = TelegramClient('scraper_session', api_id, api_hash)
            await telegram_client_instance.start(bot_token=bot_token)
            logger.info("Telegram client started successfully.")
        except Exception as e:
            logger.error(f"Failed to start Telegram client: {e}. Notifications disabled.")
            telegram_client_instance = None
    else:
        logger.warning("Telegram credentials not set. Notifications disabled.")

    if not API_URL:
        logger.error("API_URL is not set. Exiting."); return

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    try:
        async with aiohttp.ClientSession() as session:
            search_queries = [f"{c1}{c2}" for c1 in string.ascii_lowercase for c2 in string.ascii_lowercase]
            tasks = [fetch_drug_data_for_query(session, query, semaphore) for query in search_queries]
            logger.info(f"Launching {len(tasks)} API fetch tasks...")
            results = await asyncio.gather(*tasks)
            all_raw_drugs = [drug for _, batch in results for drug in batch if drug]

        logger.info(f"Fetched {len(all_raw_drugs)} raw records total.")
        
        if all_raw_drugs:
            mapped_drugs = [map_api_record_to_internal(d) for d in all_raw_drugs]
            unique_drugs_dict = {d['ID']: d for d in mapped_drugs if d and d.get('ID')}
            unique_drugs_list = list(unique_drugs_dict.values())
            logger.info(f"Processed into {len(unique_drugs_list)} unique drugs.")
            
            if unique_drugs_list:
                await process_and_commit_changes(unique_drugs_list, telegram_client_instance)
        else:
            logger.info("No drug data was fetched from the API.")
            
    except Exception as e:
        logger.exception(f"An unhandled error in the main execution loop: {e}")
    finally:
        if telegram_client_instance and telegram_client_instance.is_connected():
            await telegram_client_instance.disconnect()
            logger.info("Telegram client disconnected.")
        execution_time = time.monotonic() - script_start_time
        logger.info(f"Script finished execution in {execution_time:.2f} seconds.")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script interrupted by user.")
    except Exception as e:
        logger.critical(f"A critical error caused the script to exit: {e}", exc_info=True)
