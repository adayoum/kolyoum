import json
import csv
import datetime
import time
import asyncio
import aiohttp
import logging
import sys
import string
from typing import List, Dict, Any, Optional
import os
from supabase import create_client, Client
from collections import defaultdict
from telethon import TelegramClient
from decimal import Decimal, InvalidOperation

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# --- Configuration ---
API_URL = os.environ.get("API_URL")
DEFAULT_HEADERS = {
    "User-Agent": str(os.environ.get("USER_AGENT", "Mozilla/5.0")),
    "X-Requested-With": str(os.environ.get("X_REQUESTED_WITH", "XMLHttpRequest")),
    "Referer": str(os.environ.get("REFERER", "xx"))
}
REQUEST_TIMEOUT_SECONDS = int(os.environ.get("REQUEST_TIMEOUT_SECONDS", 10))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))
RETRY_DELAY_SECONDS = int(os.environ.get("RETRY_DELAY_SECONDS", 2))
MAX_CONCURRENT_REQUESTS = int(os.environ.get("MAX_CONCURRENT_REQUESTS", 50))

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# --- Logging Setup ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
if logger.hasHandlers():
    logger.handlers.clear()

fh = logging.FileHandler("drug_scraper_final.log", encoding='utf-8')
fh.setFormatter(formatter)
logger.addHandler(fh)

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)

# Initialize clients
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
telegram_client_instance: Optional[TelegramClient] = None

# --- Helper Functions ---
def are_values_different(val1: Any, val2: Any) -> bool:
    """Intelligently compares two values."""
    v1_is_empty = val1 is None or (isinstance(val1, str) and val1.strip() == "")
    v2_is_empty = val2 is None or (isinstance(val2, str) and val2.strip() == "")
    if v1_is_empty and v2_is_empty: return False
    try:
        return Decimal(str(val1)) != Decimal(str(val2))
    except (InvalidOperation, TypeError, ValueError):
        return str(val1).strip() != str(val2).strip()

def to_float_or_none(value: Any) -> Optional[float]:
    """Safely converts a value to a float or returns None."""
    if value is None or str(value).strip() == '': return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

# --- Main Logic ---
async def fetch_drug_data_for_query(session: aiohttp.ClientSession, search_query: str):
    """Fetches drug data for a single query."""
    payload = {"search": "1", "searchq": search_query, "order_by": "name ASC", "page": "1"}
    try:
        async with asyncio.Semaphore(MAX_CONCURRENT_REQUESTS):
            for attempt in range(MAX_RETRIES):
                try:
                    async with session.post(API_URL, data=payload, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                        response.raise_for_status()
                        return search_query, (await response.json(content_type=None)).get('data', [])
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    if attempt >= MAX_RETRIES - 1: raise
                    await asyncio.sleep(RETRY_DELAY_SECONDS)
    except Exception as e:
        logger.error(f"Query '{search_query}': Failed after all retries - {type(e).__name__}")
        return search_query, []

async def upload_to_history_async(drugs: list):
    """Uploads new or changed drug records to the database."""
    if not drugs: return
    now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()
    all_ids = [str(d["ID"]) for d in drugs if d.get("ID")]
    if not all_ids: return

    try:
        resp = await asyncio.to_thread(supabase.rpc("get_latest_record_for_ids", {"p_ids": all_ids}).execute)
        last_row_by_id = {row['id']: row for row in resp.data} if resp.data else {}
    except Exception as e:
        logger.error(f"History Upload: Could not fetch last rows. Error: {e}")
        return

    records_to_insert = []
    fields_to_check = {"Commercial Name (English)": "commercial_name_en", "Commercial Name (Arabic)": "commercial_name_ar", "Current Price": "current_price", "Previous Price": "previous_price", "Barcode": "barcode", "Dosage Form": "dosage_form"}
    
    for d in drugs:
        drug_id = str(d.get("ID"))
        if not drug_id: continue
        last_row = last_row_by_id.get(drug_id)
        if not last_row or any(are_values_different(d.get(py_key), last_row.get(db_key)) for py_key, db_key in fields_to_check.items()):
            records_to_insert.append({
                "id": drug_id, "data": now_str,
                "commercial_name_en": d.get("Commercial Name (English)"), "commercial_name_ar": d.get("Commercial Name (Arabic)"),
                "current_price": to_float_or_none(d.get("Current Price")), "previous_price": to_float_or_none(d.get("Previous Price")),
                "barcode": d.get("Barcode"), "dosage_form": d.get("Dosage Form"),
            })

    if not records_to_insert:
        logger.info("History Upload: No data changes detected. Nothing to upload.")
        return

    logger.info(f"History Upload: Found {len(records_to_insert)} new or changed records to upload.")
    for i in range(0, len(records_to_insert), 500):
        batch = records_to_insert[i:i+500]
        try:
            await asyncio.to_thread(supabase.table("history").insert(batch).execute)
            logger.info(f"History Upload: Uploaded batch {i//500+1} ({len(batch)} records)")
        except Exception as e:
            logger.error(f"History Upload: Error uploading batch {i//500+1}: {e}")

# --- Telegram Notification Logic ---
def format_change_message(change_info: Dict[str, Any]) -> str:
    """Formats a message for a detected price change."""
    curr_record = change_info['current']
    prev_record = change_info['previous']
    name_ar = curr_record.get('commercial_name_ar') or curr_record.get('commercial_name_en') or "اسم غير متوفر"
    old_price = prev_record.get('current_price')
    new_price = curr_record.get('current_price')
    old_price_str = f"{old_price:g}" if old_price is not None else "N/A"
    new_price_str = f"{new_price:g}" if new_price is not None else "N/A"
    price_change_type = "🔄 تغير"
    try:
        if float(new_price) > float(old_price): price_change_type = "⬆️ ارتفع"
        elif float(new_price) < float(old_price): price_change_type = "⬇️ انخفض"
    except (ValueError, TypeError, AttributeError): pass
    return (f"💊✨ **تحديث سعر دواء** ✨💊\n\n"
            f"**الاسم:** {name_ar}\n"
            f"**السعر:** {price_change_type}\n"
            f"   - **السعر الحالي:** {new_price_str}\n"
            f"   - **السعر السابق:** {old_price_str}")

async def send_telegram_message(message: str):
    """Sends a message to the Telegram channel using the TARGET_CHANNEL variable."""
    if not telegram_client_instance or not telegram_client_instance.is_connected():
        logger.error("Telegram client not available, cannot send message.")
        return
    try:
        target_channel = os.environ.get("TARGET_CHANNEL")
        if not target_channel:
            logger.warning("TARGET_CHANNEL not set in environment variables, cannot send message.")
            return
        entity = int(target_channel) if target_channel.lstrip('-').isdigit() else target_channel
        await telegram_client_instance.send_message(entity, message)
        logger.info(f"Notification sent to channel {target_channel}.")
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")

async def compare_history_and_notify(script_start_time: datetime.datetime):
    """Compares latest snapshots and sends notifications only for changes that are NEWER than the script start time."""
    logger.info("Checking for recent price changes to send notifications...")
    try:
        ids_resp = await asyncio.to_thread(supabase.table("history").select("id").execute)
        if not ids_resp.data: return
        all_ids = list({str(row['id']) for row in ids_resp.data})
        if not all_ids: return

        rpc_response = await asyncio.to_thread(supabase.rpc("get_latest_two_drug_history", {"p_ids": all_ids}).execute)
        if not hasattr(rpc_response, 'data') or not rpc_response.data: return

        drug_history_grouped = defaultdict(list)
        for record in rpc_response.data: drug_history_grouped[record["id"]].append(record)

        for drug_id, records in drug_history_grouped.items():
            if len(records) < 2: continue
            records.sort(key=lambda x: x['data'])
            prev_record, curr_record = records[-2], records[-1]
            
            if are_values_different(prev_record.get("current_price"), curr_record.get("current_price")):
                # *** FIX: The "Freshness Check" to prevent duplicate notifications ***
                curr_record_timestamp = datetime.datetime.fromisoformat(curr_record['data'])
                if curr_record_timestamp >= script_start_time:
                    logger.info(f"FRESH price change detected for ID {drug_id}: {prev_record.get('current_price')} -> {curr_record.get('current_price')}")
                    message = format_change_message({'previous': prev_record, 'current': curr_record})
                    await send_telegram_message(message)
                    await asyncio.sleep(1)

    except Exception as e:
        logger.exception(f"Error during history comparison for notifications: {e}")

# --- Main Execution ---
async def main():
    """Main entry point."""
    global telegram_client_instance
    # *** FIX: Record the script's start time for the "freshness check" ***
    script_start_time = datetime.datetime.now(datetime.timezone.utc)
    logger.info(f"Script starting at {script_start_time.isoformat()}")

    try:
        api_id = os.environ.get("API_ID")
        api_hash = os.environ.get("API_HASH")
        bot_token = os.environ.get("BOT_TOKEN")
        if not all([api_id, api_hash, bot_token]): raise ValueError("API_ID, API_HASH, or BOT_TOKEN not found in .env file.")
        telegram_client_instance = TelegramClient('bot_session', int(api_id), api_hash)
        await telegram_client_instance.start(bot_token=bot_token)
        logger.info("Telegram client started successfully.")
    except Exception as e:
        logger.error(f"Failed to start Telegram client: {e}. Notifications will be disabled.")
        telegram_client_instance = None

    try:
        search_queries = [a + b for a in string.ascii_lowercase for b in string.ascii_lowercase] + [str(d) for d in range(10)]
        
        async with aiohttp.ClientSession() as session:
            all_drugs = []
            for i in range(0, len(search_queries), 100):
                batch_queries = search_queries[i:i+100]
                tasks = [fetch_drug_data_for_query(session, q) for q in batch_queries]
                results = await asyncio.gather(*tasks)
                for _, drugs in results:
                    if drugs: all_drugs.extend(drugs)
        
        if all_drugs:
            unique_drugs = list({d['ID']: d for d in all_drugs if d.get('ID')}.values())
            logger.info(f"Found {len(all_drugs)} raw records, resulting in {len(unique_drugs)} unique drugs.")
            await upload_to_history_async(unique_drugs)

        # *** FIX: Pass the start time to the notification function ***
        await compare_history_and_notify(script_start_time)
    except Exception as e:
        logger.exception(f"An unhandled error occurred in main loop: {e}")
    finally:
        if telegram_client_instance and telegram_client_instance.is_connected():
            await telegram_client_instance.disconnect()
            logger.info("Telegram client disconnected.")
        logger.info("Script finished.")

if __name__ == "__main__":
    if sys.platform == 'win32': asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    start_time = time.time()
    asyncio.run(main())
    end_time = time.time()
    logger.info(f"Script completed in {end_time - start_time:.2f} seconds")
