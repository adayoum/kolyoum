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
from telethon.errors import FloodWaitError
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
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None

def parse_iso_datetime(ts_str: Any) -> Optional[datetime.datetime]:
    if not isinstance(ts_str, str): return None
    try:
        return datetime.datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None

def are_values_different(val1: Any, val2: Any) -> bool:
    if val1 is None and val2 is None: return False
    if val1 is None or val2 is None: return True
    dec1, dec2 = to_decimal_or_none(val1), to_decimal_or_none(val2)
    if dec1 is not None and dec2 is not None: return dec1 != dec2
    dt1, dt2 = parse_iso_datetime(val1), parse_iso_datetime(val2)
    if dt1 and dt2: return dt1 != dt2
    return str(val1).strip() != str(val2).strip()

def to_float_or_none(value: Any) -> Optional[float]:
    dec_val = to_decimal_or_none(value)
    return float(dec_val) if dec_val is not None else None
    
def safe_convert_timestamp(ts_str: Optional[str]) -> Optional[str]:
    if not ts_str or not str(ts_str).isdigit() or int(ts_str) == 0: return None
    try:
        ts_float = float(ts_str) / 1000.0
        dt_object = datetime.datetime.fromtimestamp(ts_float, tz=datetime.timezone.utc)
        return dt_object.isoformat()
    except (ValueError, TypeError, OSError):
        return None

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
        'Previous Price': to_float_or_none(api_record.get('oldprice')),
        'Last Price Update Date': safe_convert_timestamp(api_record.get('Date_updated')),
        'Units': api_record.get('units'),
        'Barcode': api_record.get('barcode'),
        'Dosage Form': api_record.get('dosage_form'),
        'Uses (Arabic)': api_record.get('uses'),
        'Image URL': api_record.get('img'),
    }

DB_FIELD_MAPPING = {
    'ID': 'id',
    'Commercial Name (English)': 'commercial_name_en',
    'Commercial Name (Arabic)': 'commercial_name_ar',
    'Scientific Name/Active Ingredients': 'active_ingredients',
    'Manufacturer': 'manufacturer',
    'Current Price': 'current_price',
    'Previous Price': 'previous_price',
    'Last Price Update Date': 'last_price_update_date',
    'Units': 'units',
    'Barcode': 'barcode',
    'Dosage Form': 'dosage_form',
    'Uses (Arabic)': 'uses_ar',
    'Image URL': 'image_url',
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
                    drug_list = data.get('data', [])
                    if not isinstance(drug_list, list):
                        logger.warning(f"API '{search_query}': 'data' is not a list.")
                        return search_query, []
                    return search_query, drug_list
        except Exception as e:
            wait_time = RETRY_DELAY_SECONDS * (2 ** attempt)
            logger.warning(f"API '{search_query}': Request failed ({type(e).__name__}) on attempt {attempt+1}/{MAX_RETRIES}. Retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)
    logger.error(f"API '{search_query}': All {MAX_RETRIES} retries failed.")
    return search_query, []

# --- Telegram Notification Logic ---
def format_change_message(change_info: Dict[str, Any]) -> str:
    # `current` is the new data from the API
    # `previous` is the old data from our database
    curr_record = change_info['current']
    prev_record = change_info['previous']
    
    name_ar = curr_record.get('Commercial Name (Arabic)', "اسم غير متوفر")
    name_en = curr_record.get('Commercial Name (English)', "Name not available")
    dosage_form = curr_record.get('Dosage Form', "غير محدد")
    barcode = curr_record.get('Barcode', "لا يوجد")
    
    # IMPORTANT: The keys are different for `previous` and `current`.
    # `previous` comes from the DB (lowercase), `current` comes from the Python mapping (Titlecase).
    old_price_val = prev_record.get('current_price')
    new_price_val = curr_record.get('Current Price')
    
    old_price_str = f"{old_price_val:g}" if old_price_val is not None else "N/A"
    new_price_str = f"{new_price_val:g}" if new_price_val is not None else "N/A"
    
    header = "✨ **تحديث سعر جديد** ✨"
    price_line = ""
    
    try:
        if old_price_val is not None and new_price_val is not None:
            old_p, new_p = Decimal(str(old_price_val)), Decimal(str(new_price_val))
            if new_p > old_p:
                change = ((new_p - old_p) / old_p) * 100 if old_p > 0 else float('inf')
                price_line = (f"⬆️ **السعر ارتفع:** من {old_price_str} إلى **{new_price_str}** جنيه\n"
                              f"    نسبة الزيادة: `+{change:.2f}%`")
            elif new_p < old_p:
                change = ((old_p - new_p) / old_p) * 100 if old_p > 0 else float('inf')
                price_line = (f"⬇️ **السعر انخفض:** من {old_price_str} إلى **{new_price_str}** جنيه\n"
                              f"    نسبة النقص: `-{change:.2f}%`")
            else:
                price_line = f"🔄 **السعر لم يتغير:** {new_price_str} جنيه"
        else:
            price_line = f"🔄 **السعر تغير:** من {old_price_str} إلى **{new_price_str}** جنيه"
    except (ValueError, TypeError, InvalidOperation):
        price_line = f"🔄 **السعر تغير:** من {old_price_str} إلى **{new_price_str}** جنيه"

    cairo_tz = datetime.timezone(datetime.timedelta(hours=3))
    timestamp = datetime.datetime.now(cairo_tz).strftime('%Y-%m-%d الساعة %I:%M %p (توقيت القاهرة)')
        
    return (
        f"{header}\n\n"
        f"**الاسم:** {name_ar}\n"
        f"**Name:** {name_en}\n"
        "-----------------------------------\n"
        f"{price_line}\n"
        "-----------------------------------\n"
        f"🔍 **تفاصيل إضافية:**\n"
        f"   - الشكل الدوائي: {dosage_form}\n"
        f"   - الباركود: `{barcode}`\n\n"
        f"🗓️ **وقت التحديث:** {timestamp}"
    )

async def send_telegram_message(message: str, client: TelegramClient):
    target_channel_str = os.environ.get("TARGET_CHANNEL")
    if not target_channel_str:
        logger.warning("TARGET_CHANNEL not set. Cannot send message.")
        return
    try:
        target_channel = int(target_channel_str) if target_channel_str.lstrip('-').isdigit() else target_channel_str
        await client.send_message(target_channel, message, parse_mode='md')
        logger.info(f"Notification sent successfully to channel {target_channel}.")
    except FloodWaitError as e:
        logger.warning(f"Telegram flood wait: sleeping for {e.seconds} seconds.")
        await asyncio.sleep(e.seconds + 5)
        await send_telegram_message(message, client)
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}", exc_info=True)


# --- NEW COMBINED LOGIC: Reconcile, Notify, and Upload ---
async def reconcile_and_notify(drugs: List[Dict[str, Any]], telegram_client: Optional[TelegramClient]):
    if not supabase:
        logger.warning("Supabase client not initialized. Skipping operations.")
        return
    if not drugs:
        return

    try:
        all_ids_to_check = list({str(d["ID"]) for d in drugs if d.get("ID")})
        if not all_ids_to_check: return

        logger.info(f"[Supabase] Fetching latest history for {len(all_ids_to_check)} unique drug IDs.")
        last_row_by_id = {}
        BATCH_RPC_SIZE = 1000
        for i in range(0, len(all_ids_to_check), BATCH_RPC_SIZE):
            batch_ids = all_ids_to_check[i:i + BATCH_RPC_SIZE]
            try:
                rpc_query = supabase.rpc("get_latest_record_for_ids", {"p_ids": batch_ids}).execute
                resp = await asyncio.to_thread(rpc_query)
                if resp.data:
                    for row in resp.data:
                        last_row_by_id[row['id']] = row
            except Exception as e:
                logger.error(f"Error fetching history batch {i//BATCH_RPC_SIZE + 1}: {e}. This batch skipped.")
        
        logger.info(f"[Supabase] Retrieved {len(last_row_by_id)} existing history records.")
        
        records_to_insert = []
        notifications_sent = 0

        for drug_data in drugs:
            drug_id = str(drug_data.get("ID"))
            if not drug_id: continue

            last_db_record = last_row_by_id.get(drug_id)
            
            if not last_db_record:
                logger.info(f"Drug ID {drug_id}: New drug detected. Adding to insert list.")
                records_to_insert.append(drug_data)
                continue

            # Check if any value has changed
            has_changed = any(
                are_values_different(drug_data.get(map_key), last_db_record.get(db_key))
                for map_key, db_key in DB_FIELD_MAPPING.items()
            )

            if has_changed:
                logger.info(f"Drug ID {drug_id}: Change detected. Preparing to upload and notify if price changed.")
                records_to_insert.append(drug_data)

                # YOUR LOGIC: Check for price change and notify IMMEDIATELY
                if are_values_different(drug_data.get("Current Price"), last_db_record.get("current_price")):
                    logger.info(f"Price change confirmed for ID {drug_id}: DB='{last_db_record.get('current_price')}' -> API='{drug_data.get('Current Price')}'")
                    if telegram_client and telegram_client.is_connected():
                        message = format_change_message({'previous': last_db_record, 'current': drug_data})
                        await send_telegram_message(message, telegram_client)
                        notifications_sent += 1
                        await asyncio.sleep(1.5) # Pause between notifications
        
        if not records_to_insert:
            logger.info("No new or changed records to upload. All data is up-to-date.")
            return

        logger.info(f"Found {len(records_to_insert)} new/changed records. Uploading to database...")
        logger.info(f"Total notifications sent in this run: {notifications_sent}")
        
        supabase_records = []
        for record in records_to_insert:
            new_row = {"scraped_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}
            for map_key, db_key in DB_FIELD_MAPPING.items():
                new_row[db_key] = record.get(map_key)
            supabase_records.append(new_row)
            
        BATCH_INSERT_SIZE = 500
        for i in range(0, len(supabase_records), BATCH_INSERT_SIZE):
            batch = supabase_records[i:i+BATCH_INSERT_SIZE]
            try:
                insert_query = supabase.table("history").insert(batch).execute
                await asyncio.to_thread(insert_query)
                logger.info(f"History Upload: Uploaded batch {i//BATCH_INSERT_SIZE+1} ({len(batch)} records).")
            except Exception as e:
                logger.error(f"History Upload: Error uploading batch {i//BATCH_INSERT_SIZE+1}: {e}")

    except Exception as e:
        logger.exception(f"An unhandled error occurred during reconcile_and_notify: {e}")

# --- Main Execution ---
async def main():
    script_start_time = time.monotonic()
    logger.info(f"Script starting at {datetime.datetime.now(datetime.timezone.utc).isoformat()}...")

    telegram_client_instance: Optional[TelegramClient] = None
    api_id_str, api_hash, bot_token = os.environ.get("API_ID"), os.environ.get("API_HASH"), os.environ.get("BOT_TOKEN")
    if all([api_id_str, api_hash, bot_token]):
        try:
            api_id = int(api_id_str)
            telegram_client_instance = TelegramClient('scraper_session', api_id, api_hash, sequential_updates=True)
            logger.info("Starting Telegram client...")
            
            for attempt in range(3):
                try:
                    await telegram_client_instance.start(bot_token=bot_token)
                    break
                except FloodWaitError as e:
                    if e.seconds > 120:
                        logger.error(f"Telegram FloodWait is too long ({e.seconds}s). Disabling notifications.")
                        raise
                    logger.warning(f"Telegram FloodWait of {e.seconds}s detected. Waiting... (Attempt {attempt + 1}/3)")
                    await asyncio.sleep(e.seconds + 5)
            
            if not await telegram_client_instance.is_user_authorized():
                 raise Exception("Telegram authorization failed after attempts.")
            logger.info("Telegram client started and authorized successfully.")

        except Exception as e:
            logger.error(f"Failed to start Telegram client: {type(e).__name__}: {e}. Notifications disabled.")
            if telegram_client_instance and telegram_client_instance.is_connected():
                await telegram_client_instance.disconnect()
            telegram_client_instance = None
    else:
        logger.warning("Telegram credentials not fully set. Notifications disabled.")

    if not API_URL:
        logger.error("API_URL is not set. Cannot fetch drug data. Exiting.")
        return

    all_raw_drugs = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    try:
        async with aiohttp.ClientSession() as session:
            search_queries = [f"{c1}{c2}" for c1 in string.ascii_lowercase for c2 in string.ascii_lowercase]
            tasks = [fetch_drug_data_for_query(session, query, semaphore) for query in search_queries]
            
            logger.info(f"Launching {len(tasks)} tasks to fetch data...")
            results = await asyncio.gather(*tasks)
            
            for _, drugs_from_batch in results:
                if drugs_from_batch:
                    all_raw_drugs.extend(drugs_from_batch)

        logger.info(f"Finished fetching API data. Found {len(all_raw_drugs)} raw records total.")
        
        if all_raw_drugs:
            mapped_drugs = [map_api_record_to_internal(d) for d in all_raw_drugs if d]
            valid_mapped_drugs = [d for d in mapped_drugs if d]
            unique_drugs_dict = {d['ID']: d for d in valid_mapped_drugs if d.get('ID')}
            unique_drugs_list = list(unique_drugs_dict.values())
            logger.info(f"Processed into {len(unique_drugs_list)} unique drugs.")
            
            if unique_drugs_list:
                # The single, combined function call
                await reconcile_and_notify(unique_drugs_list, telegram_client_instance)
        else:
            logger.info("No drug data was fetched from the API across all queries.")
            
    except Exception as e:
        logger.exception(f"An unhandled error occurred in the main execution loop: {e}")
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
