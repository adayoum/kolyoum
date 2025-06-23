import json
import csv
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
from collections import defaultdict
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
    "Referer": str(os.environ.get("REFERER", "xx"))
}
REQUEST_TIMEOUT_SECONDS: int = int(os.environ.get("REQUEST_TIMEOUT_SECONDS", 15))
MAX_RETRIES: int = int(os.environ.get("MAX_RETRIES", 5))
RETRY_DELAY_SECONDS: int = int(os.environ.get("RETRY_DELAY_SECONDS", 2))
MAX_CONCURRENT_REQUESTS: int = int(os.environ.get("MAX_CONCURRENT_REQUESTS", 5))

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

# --- Supabase Key Check ---
def log_supabase_key_type(key: Optional[str]):
    if not key:
        logger.warning("SUPABASE_KEY is empty! Check your .env file.")
        return
    if key.startswith("eyJ") and len(key) > 60:
        if "anon" in key:
            logger.warning("Supabase key appears to be an anon/public key. You MUST use the service_role key for inserts!")
        else:
            logger.info("Supabase key appears to be a service_role (secret) key.")
    else:
        logger.warning("Supabase key format is unusual. Double-check that you are using the service_role key.")

log_supabase_key_type(SUPABASE_KEY)

# Initialize Supabase client
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
def are_values_different(val1: Any, val2: Any) -> bool:
    v1_is_empty = val1 is None or (isinstance(val1, str) and val1.strip() == "")
    v2_is_empty = val2 is None or (isinstance(val2, str) and val2.strip() == "")
    if v1_is_empty and v2_is_empty: return False
    if v1_is_empty or v2_is_empty: return True
    try:
        return Decimal(str(val1)) != Decimal(str(val2))
    except (InvalidOperation, TypeError, ValueError):
        return str(val1).strip() != str(val2).strip()

def to_float_or_none(value: Any) -> Optional[float]:
    if value is None or (isinstance(value, str) and value.strip() == ''): return None
    try:
        return float(Decimal(str(value)))
    except (ValueError, TypeError, InvalidOperation):
        return None

def safe_convert_timestamp(ts_str: Optional[str]) -> Optional[str]:
    if not ts_str or not str(ts_str).isdigit() or int(ts_str) == 0:
        return None
    try:
        ts_float = float(ts_str) / 1000.0
        dt_object = datetime.datetime.fromtimestamp(ts_float, tz=datetime.timezone.utc)
        return dt_object.isoformat()
    except (ValueError, TypeError, OSError):
        logger.warning(f"Could not convert timestamp string: {ts_str}")
        return None

# --- Data Mapping ---
def map_api_record_to_internal(api_record: dict) -> Optional[Dict[str, Any]]:
    if not api_record or not api_record.get('id'):
        return None
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
    'Current Price': 'current_price',
    'Previous Price': 'previous_price',
    'Last Price Update Date': 'last_price_update_date',
    'Barcode': 'barcode',
    'Dosage Form': 'dosage_form',
}

# --- API Fetching Logic ---
async def fetch_drug_data_for_query(
    session: aiohttp.ClientSession,
    search_query: str,
    semaphore: asyncio.Semaphore
) -> Tuple[str, List[Dict[str, Any]]]:
    payload = {"search": "1", "searchq": search_query, "order_by": "name ASC", "page": "1"}
    for attempt in range(MAX_RETRIES):
        try:
            async with semaphore:
                async with session.post(API_URL, data=payload, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                    response.raise_for_status()
                    try:
                        data = await response.json(content_type=None)
                    except aiohttp.ContentTypeError:
                        logger.warning(f"API '{search_query}': Unexpected content type. Response text: {await response.text()[:200]}...")
                        return search_query, []

                    drug_list = data.get('data', [])
                    if not isinstance(drug_list, list):
                        logger.warning(f"API '{search_query}': 'data' field is not a list. Found: {type(drug_list)}. Response: {str(data)[:200]}...")
                        return search_query, []

                    count = len(drug_list)
                    logger.info(f"API '{search_query}': {count} results returned.")
                    return search_query, drug_list
        except (aiohttp.ClientError, asyncio.TimeoutError, aiohttp.ServerDisconnectedError) as e:
            wait_time = RETRY_DELAY_SECONDS * (2 ** attempt)
            logger.warning(f"API '{search_query}': Request failed ({type(e).__name__}) on attempt {attempt+1}/{MAX_RETRIES}. Retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)
        except Exception as e:
            logger.error(f"API '{search_query}': Unexpected error on attempt {attempt+1}/{MAX_RETRIES} - {type(e).__name__}: {e}")
            if attempt == MAX_RETRIES - 1:
                logger.error(f"API '{search_query}': All retries failed after unexpected error: {e}")
                return search_query, []
            await asyncio.sleep(RETRY_DELAY_SECONDS)
    logger.error(f"API '{search_query}': All {MAX_RETRIES} retries failed.")
    return search_query, []

# --- Supabase Upload Logic (FINAL REVISED LOGIC) ---
async def upload_to_history_async(drugs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not supabase:
        logger.warning("Supabase client not initialized. Skipping upload.")
        return []
    if not drugs:
        logger.info("No drugs to process for upload.")
        return []

    try:
        logger.info(f"[Supabase] Preparing to fetch history for {len(drugs)} potential drugs.")
        all_ids_to_check = list({str(d["ID"]) for d in drugs if d.get("ID")})
        if not all_ids_to_check: return []

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
                logger.error(f"Error fetching history batch {i//BATCH_RPC_SIZE + 1}: {e}. This batch will be skipped.")
        
        logger.info(f"[Supabase] Retrieved {len(last_row_by_id)} existing history records.")
        
        records_to_insert = []
        for drug_data in drugs:
            if not drug_data: continue
            drug_id = str(drug_data.get("ID"))
            if not drug_id: continue

            last_db_record = last_row_by_id.get(drug_id)
            
            if not last_db_record:
                logger.info(f"New drug detected: ID {drug_id}. Adding to history.")
                records_to_insert.append(drug_data)
                continue

            api_update_date_str = drug_data.get('Last Price Update Date')
            db_update_date_str = last_db_record.get('last_price_update_date')

            if api_update_date_str and db_update_date_str and api_update_date_str > db_update_date_str:
                logger.info(f"Genuine change for ID {drug_id} detected by newer update date.")
                records_to_insert.append(drug_data)
                continue

            has_changed = any(
                are_values_different(drug_data.get(map_key), last_db_record.get(db_key))
                for map_key, db_key in DB_FIELD_MAPPING.items() if map_key != 'ID'
            )

            if has_changed:
                logger.info(f"Change for ID {drug_id} detected by value comparison (manual edit or same date).")
                records_to_insert.append(drug_data)
            
        if not records_to_insert:
            logger.info("History Upload: No new or changed records detected. All data is up-to-date.")
            return []

        logger.info(f"History Upload: Found {len(records_to_insert)} new or changed records to upload.")
        
        supabase_records = []
        for record in records_to_insert:
            new_row = {"data": datetime.datetime.now(datetime.timezone.utc).isoformat()}
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
        
        return records_to_insert

    except Exception as e:
        logger.exception(f"An unhandled error occurred during Supabase upload logic: {e}")
        return []

# --- Telegram Notification Logic ---
def format_change_message(change_info: Dict[str, Any]) -> str:
    curr_record = change_info['current']
    prev_record = change_info['previous']
    
    name_ar = curr_record.get('Commercial Name (Arabic)') or "اسم غير متوفر"
    name_en = curr_record.get('Commercial Name (English)') or "Name not available"
    dosage_form = curr_record.get('Dosage Form') or "غير محدد"
    barcode = curr_record.get('Barcode') or "لا يوجد"
    
    old_price_val = prev_record.get('current_price')
    new_price_val = curr_record.get('Current Price')
    
    old_price_str = f"{old_price_val:g}" if old_price_val is not None else "N/A"
    new_price_str = f"{new_price_val:g}" if new_price_val is not None else "N/A"
    
    header = "✨ تحديث سعر جديد ✨"
    price_line = ""
    
    try:
        if old_price_val is not None and new_price_val is not None:
            old_p = Decimal(str(old_price_val))
            new_p = Decimal(str(new_price_val))
            
            if new_p > old_p:
                change_percentage = ((new_p - old_p) / old_p) * 100 if old_p > 0 else float('inf')
                price_line = (f"⬆️ **السعر زاد:** من {old_price_str} إلى **{new_price_str}** جنيه\n"
                              f"   نسبة الزيادة: `+{change_percentage:.2f}%`")
            elif new_p < old_p:
                change_percentage = ((old_p - new_p) / old_p) * 100 if old_p > 0 else float('inf')
                price_line = (f"⬇️ **السعر قل:** من {old_price_str} إلى **{new_price_str}** جنيه\n"
                              f"   نسبة النقص: `-{change_percentage:.2f}%`")
            else:
                price_line = f"🔄 **السعر ثابت:** {new_price_str} جنيه"
        else:
            price_line = f"🔄 **السعر اتغير:** من {old_price_str} إلى **{new_price_str}** جنيه"
            
    except (ValueError, TypeError, InvalidOperation):
        price_line = f"🔄 **السعر اتغير:** من {old_price_str} إلى **{new_price_str}** جنيه"

    cairo_tz = datetime.timezone(datetime.timedelta(hours=3))
    timestamp = datetime.datetime.now(cairo_tz).strftime('%Y-%m-%d الساعة %I:%M %p')
        
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

async def send_telegram_message(message: str, client: Optional[TelegramClient]):
    if not client or not client.is_connected():
        logger.warning("Telegram client not available, cannot send message.")
        return
    target_channel_str = os.environ.get("TARGET_CHANNEL")
    if not target_channel_str:
        logger.warning("TARGET_CHANNEL not set.")
        return
    try:
        target_channel = int(target_channel_str) if target_channel_str.lstrip('-').isdigit() else target_channel_str
        await client.send_message(target_channel, message, parse_mode='md')
        logger.info(f"Notification sent to channel {target_channel}.")
    except FloodWaitError as e:
        logger.warning(f"Telegram flood wait error: {e}. Retrying in {e.seconds}s.")
        await asyncio.sleep(e.seconds)
        await send_telegram_message(message, client)
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")

async def compare_and_notify_changes(changed_records: List[Dict[str, Any]], telegram_client: Optional[TelegramClient]):
    if not changed_records:
        logger.info("No changed records to notify about.")
        return
    if not telegram_client or not telegram_client.is_connected():
        logger.warning("Telegram client not ready. Skipping notification check.")
        return

    logger.info(f"Checking {len(changed_records)} changed records for price updates...")
    
    changed_ids = [record['ID'] for record in changed_records if record.get('ID')]
    if not changed_ids: return

    try:
        rpc_query = supabase.rpc("get_penultimate_drug_history", {"p_ids": changed_ids}).execute
        rpc_response = await asyncio.to_thread(rpc_query)

        if not hasattr(rpc_response, 'data') or not rpc_response.data:
            logger.warning("Could not fetch previous history for changed records.")
            return

        prev_history_by_id = {row['id']: row for row in rpc_response.data}
        notifications_sent = 0

        for curr_record_map in changed_records:
            drug_id = curr_record_map.get('ID')
            if not drug_id: continue

            prev_record_db = prev_history_by_id.get(drug_id)
            if not prev_record_db:
                continue

            if are_values_different(curr_record_map.get("Current Price"), prev_record_db.get("current_price")):
                logger.info(f"Price change confirmed for ID {drug_id}: {prev_record_db.get('current_price')} -> {curr_record_map.get('Current Price')}")
                message = format_change_message({'previous': prev_record_db, 'current': curr_record_map})
                await send_telegram_message(message, telegram_client)
                notifications_sent += 1
                await asyncio.sleep(1)

        logger.info(f"Finished notification check. Sent {notifications_sent} notifications.")
    except Exception as e:
        logger.exception(f"Error during notification checks: {e}")

# --- Main Execution (with FloodWaitError handling) ---
async def main():
    script_start_time = datetime.datetime.now(datetime.timezone.utc)
    logger.info(f"Script starting at {script_start_time.isoformat()}...")

    telegram_client_instance: Optional[TelegramClient] = None
    try:
        api_id_str, api_hash, bot_token = os.environ.get("API_ID"), os.environ.get("API_HASH"), os.environ.get("BOT_TOKEN")
        if all([api_id_str, api_hash, bot_token]):
            api_id = int(api_id_str)
            telegram_client_instance = TelegramClient('scraper_session', api_id, api_hash)
            
            for i in range(3):
                try:
                    await telegram_client_instance.start(bot_token=bot_token)
                    if await telegram_client_instance.is_user_authorized():
                        logger.info("Telegram client started and authorized successfully.")
                        break
                    else:
                        logger.error(f"Telegram client failed to authorize on attempt {i+1}.")
                except FloodWaitError as e:
                    logger.warning(f"Telegram flood wait on startup: Must wait {e.seconds}s. Attempt {i+1}/3.")
                    if i < 2:
                        await asyncio.sleep(e.seconds + 5)
                    else:
                        raise
            
            if not telegram_client_instance.is_connected():
                 logger.error("Could not establish Telegram connection after retries. Notifications disabled.")
                 telegram_client_instance = None
        else:
            logger.warning("Telegram credentials not fully set. Notifications disabled.")
    except Exception as e:
        logger.error(f"Failed to start Telegram client: {e}. Notifications disabled.")
        if telegram_client_instance and telegram_client_instance.is_connected():
            await telegram_client_instance.disconnect()
        telegram_client_instance = None

    if not API_URL:
        logger.error("API_URL is not set. Cannot fetch drug data.")
        if telegram_client_instance and telegram_client_instance.is_connected():
            await telegram_client_instance.disconnect()
        return

    all_raw_drugs = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    try:
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_drug_data_for_query(session, f"{c1}{c2}", semaphore) for c1 in string.ascii_lowercase for c2 in string.ascii_lowercase]
            results = await asyncio.gather(*tasks)
            for _, drugs_from_batch in results:
                if drugs_from_batch: all_raw_drugs.extend(drugs_from_batch)

        logger.info(f"Finished fetching data from API. Found {len(all_raw_drugs)} raw records.")
        if all_raw_drugs:
            logger.info(f"Sample raw drug record: {all_raw_drugs[0] if all_raw_drugs else 'N/A'}")
            
            mapped_drugs = [map_api_record_to_internal(d) for d in all_raw_drugs if d]
            valid_mapped_drugs = [d for d in mapped_drugs if d]
            
            unique_drugs_dict = {d['ID']: d for d in valid_mapped_drugs if d.get('ID')}
            unique_drugs_list = list(unique_drugs_dict.values())
            logger.info(f"Processed {len(valid_mapped_drugs)} records, resulting in {len(unique_drugs_list)} unique drugs.")
            
            if unique_drugs_list:
                inserted_records = await upload_to_history_async(unique_drugs_list)
                await compare_and_notify_changes(inserted_records, telegram_client_instance)
            else:
                logger.warning("No unique drugs found to upload.")
        else:
            logger.info("No drug data was fetched from the API.")
            
    except Exception as e:
        logger.exception(f"An unhandled error occurred in the main execution loop: {e}")
    finally:
        if telegram_client_instance and telegram_client_instance.is_connected():
            await telegram_client_instance.disconnect()
            logger.info("Telegram client disconnected.")
        logger.info("Script finished execution.")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    start_time = time.time()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script interrupted by user.")
    finally:
        end_time = time.time()
        logger.info(f"Script completed in {end_time - start_time:.2f} seconds.")
