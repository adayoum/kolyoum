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
from telethon.errors import FloodWaitError, SessionPasswordNeededError, AuthKeyError
from decimal import Decimal, InvalidOperation

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# --- Configuration ---
API_URL: Optional[str] = os.environ.get("API_URL")
DEFAULT_HEADERS: Dict[str, str] = {
    "User-Agent": str(os.environ.get("USER_AGENT", "Mozilla/5.0")),
    "X-Requested-With": str(os.environ.get("X_REQUESTED_WITH", "XMLHttpRequest")),
    "Referer": str(os.environ.get("REFERER", "xx")) # Consider a more realistic or variable referer
}
REQUEST_TIMEOUT_SECONDS: int = int(os.environ.get("REQUEST_TIMEOUT_SECONDS", 15)) # Slightly increased timeout
MAX_RETRIES: int = int(os.environ.get("MAX_RETRIES", 5)) # Increased retries
RETRY_DELAY_SECONDS: int = int(os.environ.get("RETRY_DELAY_SECONDS", 2))
MAX_CONCURRENT_REQUESTS: int = int(os.environ.get("MAX_CONCURRENT_REQUESTS", 5))
BATCH_SIZE: int = 26 * 26 # You have 26*26 queries, so make batch size match if possible, or process all at once if not too many

SUPABASE_URL: Optional[str] = os.environ.get("SUPABASE_URL")
SUPABASE_KEY: Optional[str] = os.environ.get("SUPABASE_KEY")

# --- Logging Setup ---
logger = logging.getLogger(__name__) # Use __name__ for logger name
logger.setLevel(logging.INFO)

# Prevent duplicate handlers if script is reloaded
if not logger.handlers:
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    fh = logging.FileHandler("drug_scraper_final.log", encoding='utf-8')
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
# It's better to initialize this after checking SUPABASE_URL and SUPABASE_KEY
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
    """Intelligently compares two values, handling potential None or empty strings."""
    v1_is_empty = val1 is None or (isinstance(val1, str) and val1.strip() == "")
    v2_is_empty = val2 is None or (isinstance(val2, str) and val2.strip() == "")
    if v1_is_empty and v2_is_empty:
        return False # Both are considered empty, so no difference.
    if v1_is_empty or v2_is_empty:
        return True # One is empty, the other is not.

    try:
        # Use Decimal for accurate price comparisons
        return Decimal(str(val1)) != Decimal(str(val2))
    except (InvalidOperation, TypeError, ValueError):
        # Fallback to string comparison if Decimal conversion fails
        return str(val1).strip() != str(val2).strip()

def to_float_or_none(value: Any) -> Optional[float]:
    """Safely converts a value to a float or returns None."""
    if value is None or (isinstance(value, str) and value.strip() == ''):
        return None
    try:
        # Attempt to parse as Decimal first for better precision before converting to float
        return float(Decimal(str(value)))
    except (ValueError, TypeError, InvalidOperation):
        return None

# --- API Fetching Logic ---
async def fetch_drug_data_for_query(
    session: aiohttp.ClientSession,
    search_query: str,
    semaphore: asyncio.Semaphore
) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Fetches drug data for a single query with exponential backoff and semaphore.
    Returns the search query and the list of drug data.
    """
    payload = {"search": "1", "searchq": search_query, "order_by": "name ASC", "page": "1"}
    for attempt in range(MAX_RETRIES):
        try:
            async with semaphore: # Acquire semaphore before making the request
                async with session.post(API_URL, data=payload, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                    response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
                    # Try to parse as JSON, but handle cases where content_type might be unexpected
                    try:
                        data = await response.json(content_type=None)
                    except aiohttp.ContentTypeError:
                        logger.warning(f"API '{search_query}': Unexpected content type. Response text: {await response.text()[:200]}...") # Log snippet of response
                        return search_query, [] # Skip this query if content type is wrong

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
            # If it's an unknown error, we might want to stop retrying or log more details.
            # For now, we'll continue with retries as it might be transient.
            if attempt == MAX_RETRIES - 1:
                logger.error(f"API '{search_query}': All retries failed after unexpected error: {e}")
                return search_query, []
            await asyncio.sleep(RETRY_DELAY_SECONDS) # Short delay before next retry for unexpected errors
    logger.error(f"API '{search_query}': All {MAX_RETRIES} retries failed.")
    return search_query, []

# --- Supabase Upload Logic ---
async def upload_to_history_async(drugs: List[Dict[str, Any]]):
    """
    Uploads new or changed drug records to the 'history' table in Supabase.
    If the 'history' table is empty, it populates it with all found drugs.
    Otherwise, it checks for changes and inserts only modified records.
    """
    if not supabase:
        logger.warning("Supabase client not initialized. Skipping upload_to_history_async.")
        return
    if not drugs:
        logger.info("upload_to_history_async: No drugs to process.")
        return

    all_ids = [str(d["ID"]) for d in drugs if d.get("ID")]
    if not all_ids:
        logger.info("upload_to_history_async: No valid IDs found in drugs.")
        return

    # --- Check if history table is empty ---
    try:
        # Fetch a count of existing records to determine if the table is empty
        count_resp = await asyncio.to_thread(
            supabase.table("history").select("id").limit(1).execute() # Limit to 1 to check for existence efficiently
        )
        is_history_empty = not count_resp.data # If data is empty list, table is empty
        
        if is_history_empty:
            logger.info("History table is empty. Populating with all found drugs.")
            records_to_insert = [] # Rename for clarity in this block
            for drug in drugs:
                drug_id = str(drug.get("ID"))
                if not drug_id:
                    logger.warning("Skipping drug with no ID during initial population.")
                    continue
                
                record_data = {
                    "id": drug_id,
                    "data": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                }
                # Populate all relevant fields for the initial insert
                fields_to_check_mapping = {
                    "ID": "id", # Primary key
                    "Commercial Name (English)": "commercial_name_en",
                    "Commercial Name (Arabic)": "commercial_name_ar",
                    "Current Price": "current_price",
                    "Previous Price": "previous_price",
                    "Barcode": "barcode",
                    "Dosage Form": "dosage_form",
                }
                for api_key, db_key in fields_to_check_mapping.items():
                    if db_key == "id": continue
                    value = drug.get(api_key)
                    if db_key in ["current_price", "previous_price"]:
                        record_data[db_key] = to_float_or_none(value)
                    else:
                        record_data[db_key] = value if value is not None else None
                records_to_insert.append(record_data)

            if not records_to_insert:
                logger.info("No valid records to insert during initial population.")
                return

            logger.info(f"Initial population: Preparing to insert {len(records_to_insert)} records.")
            # Batch insert for initial population
            BATCH_INSERT_SIZE = 500
            for i in range(0, len(records_to_insert), BATCH_INSERT_SIZE):
                batch = records_to_insert[i:i+BATCH_INSERT_SIZE]
                try:
                    # Use insert here as it's the initial population
                    await asyncio.to_thread(supabase.table("history").insert(batch).execute)
                    logger.info(f"Initial Population: Uploaded batch {i//BATCH_INSERT_SIZE+1} ({len(batch)} records) to Supabase.")
                except Exception as e:
                    logger.error(f"Initial Population: Error uploading batch {i//BATCH_INSERT_SIZE+1}: {e}")
            return # Exit after initial population
        
        # --- If history table is NOT empty, proceed with checking for changes ---
        logger.info(f"[Supabase] Fetching {len(all_ids)} latest history records for comparison...")
        resp = await asyncio.to_thread(
            supabase.rpc("get_latest_record_for_ids", {"p_ids": all_ids}).execute
        )
        last_row_by_id = {row['id']: row for row in resp.data} if resp.data else {}
        logger.info(f"[Supabase] Retrieved {len(last_row_by_id)} existing history records.")

        records_to_insert_or_update = []
        fields_to_check_mapping = {
            "ID": "id",
            "Commercial Name (English)": "commercial_name_en",
            "Commercial Name (Arabic)": "commercial_name_ar",
            "Current Price": "current_price",
            "Previous Price": "previous_price",
            "Barcode": "barcode",
            "Dosage Form": "dosage_form",
        }

        for drug in drugs:
            drug_id = str(drug.get("ID"))
            if not drug_id:
                logger.warning("Skipping drug with no ID.")
                continue

            last_row = last_row_by_id.get(drug_id)
            
            # If a record with this ID doesn't exist in history, it means it's a new drug
            # that wasn't in the table before. We should insert it.
            is_new_record = not last_row

            has_changed = False
            if not is_new_record: # Only check for changes if the record already exists
                for api_key, db_key in fields_to_check_mapping.items():
                    if db_key == "id": continue
                    if are_values_different(drug.get(api_key), last_row.get(db_key)):
                        has_changed = True
                        break

            # Add to the list if it's a brand new drug OR if an existing drug has changed
            if is_new_record or has_changed:
                record_data = {
                    "id": drug_id,
                    "data": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                }
                for api_key, db_key in fields_to_check_mapping.items():
                    if db_key == "id": continue
                    value = drug.get(api_key)
                    if db_key in ["current_price", "previous_price"]:
                        record_data[db_key] = to_float_or_none(value)
                    else:
                        record_data[db_key] = value if value is not None else None
                records_to_insert_or_update.append(record_data)

        if not records_to_insert_or_update:
            logger.info("History Upload: No new or changed records detected. Nothing to upload.")
            return

        logger.info(f"History Upload: Found {len(records_to_insert_or_update)} new or changed records to upload.")
        
        BATCH_INSERT_SIZE = 500
        for i in range(0, len(records_to_insert_or_update), BATCH_INSERT_SIZE):
            batch = records_to_insert_or_update[i:i+BATCH_INSERT_SIZE]
            try:
                # Use insert here, as we've already filtered for new/changed records.
                # If a record truly has no changes, it won't be in this batch.
                await asyncio.to_thread(supabase.table("history").insert(batch).execute)
                logger.info(f"History Upload: Uploaded batch {i//BATCH_INSERT_SIZE+1} ({len(batch)} records) to Supabase.")
            except Exception as e:
                logger.error(f"History Upload: Error uploading batch {i//BATCH_INSERT_SIZE+1}: {e}")

    except Exception as e:
        logger.exception(f"An error occurred during Supabase upload logic: {e}")

# --- Telegram Notification Logic ---
def format_change_message(change_info: Dict[str, Any]) -> str:
    """Formats a message for a detected price change, suitable for Telegram."""
    curr_record = change_info['current']
    prev_record = change_info['previous']
    
    name_ar = curr_record.get('commercial_name_ar') or curr_record.get('commercial_name_en') or "اسم غير متوفر"
    old_price_val = prev_record.get('current_price')
    new_price_val = curr_record.get('current_price')
    
    old_price_str = f"{old_price_val:g}" if old_price_val is not None else "N/A"
    new_price_str = f"{new_price_val:g}" if new_price_val is not None else "N/A"
    
    price_change_type = "🔄 تغير"
    try:
        # Ensure values are floats for comparison if they are not None
        if old_price_val is not None and new_price_val is not None:
            old_price_float = float(old_price_val)
            new_price_float = float(new_price_val)
            if new_price_float > old_price_float:
                price_change_type = "⬆️ ارتفع"
            elif new_price_float < old_price_float:
                price_change_type = "⬇️ انخفض"
    except (ValueError, TypeError):
        # If conversion fails, stick with the generic change type
        pass
        
    return (
        f"💊✨ **تحديث سعر دواء** ✨💊\n\n"
        f"**الاسم:** {name_ar}\n"
        f"**السعر:** {price_change_type}\n"
        f"   - **السعر الحالي:** {new_price_str}\n"
        f"   - **السعر السابق:** {old_price_str}"
    )

async def send_telegram_message(message: str, client: Optional[TelegramClient]):
    """Sends a message to the Telegram channel using the TARGET_CHANNEL variable."""
    if not client or not client.is_connected():
        logger.warning("Telegram client not available or not connected, cannot send message.")
        return
    
    target_channel = os.environ.get("TARGET_CHANNEL")
    if not target_channel:
        logger.warning("TARGET_CHANNEL not set in environment variables, cannot send message.")
        return
        
    try:
        # Try converting to integer for chat ID, otherwise use as username/channel ID string
        entity = int(target_channel) if target_channel.lstrip('-').isdigit() else target_channel
        await client.send_message(entity, message)
        logger.info(f"Notification sent successfully to channel {target_channel}.")
    except FloodWaitError as e:
        logger.warning(f"Telegram flood wait error: {e}. Will retry later if needed.")
        await asyncio.sleep(e.seconds)
        # Optionally retry sending the message here
        await send_telegram_message(message, client)
    except Exception as e:
        logger.error(f"Failed to send Telegram message to {target_channel}: {e}")

async def compare_history_and_notify(script_start_time: datetime.datetime, telegram_client: Optional[TelegramClient]):
    """
    Compares latest drug history records and sends Telegram notifications for
    price changes that occurred since the script started.
    """
    if not telegram_client or not telegram_client.is_connected():
        logger.info("Telegram client not ready. Skipping notification check.")
        return
        
    logger.info("Checking for recent price changes to send notifications...")
    try:
        # Fetch all unique drug IDs present in the history table
        ids_resp = await asyncio.to_thread(supabase.table("history").select("id").execute)
        if not ids_resp.data:
            logger.info("No IDs found in history table. Cannot check for changes.")
            return
        
        all_ids = list({str(row['id']) for row in ids_resp.data})
        if not all_ids:
            logger.info("No valid IDs retrieved from history table.")
            return

        # Fetch the latest two records for each drug ID
        # Adjust the RPC call or query to fetch the necessary data.
        # Assuming 'get_latest_two_drug_history' returns records sorted by 'data' (timestamp)
        # If your RPC does not sort, you might need to fetch more and sort manually.
        rpc_response = await asyncio.to_thread(
            supabase.rpc("get_latest_two_drug_history", {"p_ids": all_ids}).execute
        )
        
        if not hasattr(rpc_response, 'data') or not rpc_response.data:
            logger.info("No history data returned from RPC.")
            return

        drug_history_grouped = defaultdict(list)
        for record in rpc_response.data:
            drug_history_grouped[record["id"]].append(record)

        notifications_sent = 0
        for drug_id, records in drug_history_grouped.items():
            if len(records) < 2:
                continue # Need at least two records to compare

            # Ensure records are sorted by timestamp (most recent last)
            records.sort(key=lambda x: x['data'])
            
            prev_record, curr_record = records[-2], records[-1]

            # Check if the current price has changed between the two latest records
            if are_values_different(prev_record.get("current_price"), curr_record.get("current_price")):
                try:
                    curr_record_timestamp = datetime.datetime.fromisoformat(curr_record['data'])
                    # Only send notification if the change happened AFTER the script started
                    if curr_record_timestamp >= script_start_time:
                        logger.info(f"FRESH price change detected for Drug ID {drug_id} (Time: {curr_record['data']}). Prev: {prev_record.get('current_price')} -> Curr: {curr_record.get('current_price')}")
                        message = format_change_message({'previous': prev_record, 'current': curr_record})
                        await send_telegram_message(message, telegram_client)
                        notifications_sent += 1
                        await asyncio.sleep(1) # Small delay between notifications to avoid being rate-limited by Telegram
                except ValueError:
                    logger.warning(f"Could not parse timestamp for record ID {drug_id}: {curr_record.get('data')}")
                except Exception as e:
                    logger.error(f"Error processing notification for drug ID {drug_id}: {e}")
        
        logger.info(f"Finished checking for notifications. {notifications_sent} notifications sent.")

    except Exception as e:
        logger.exception(f"An unhandled error occurred during history comparison and notification: {e}")

# --- Main Execution ---

def map_api_record_to_internal(api_record: dict) -> Dict[str, Any]:
    """Maps API record fields to the expected internal schema."""
    return {
        "ID": api_record.get("id"),
        "Commercial Name (English)": api_record.get("name"),
        "Commercial Name (Arabic)": api_record.get("arabic"),
        "Current Price": api_record.get("price"),
        "Previous Price": api_record.get("oldprice"),
        "Barcode": api_record.get("barcode"),
        "Dosage Form": api_record.get("dosage_form"),
        # Add more mappings if needed based on your API response
    }

async def main():
    """Main entry point for the script."""
    script_start_time = datetime.datetime.now(datetime.timezone.utc)
    logger.info(f"Script starting at {script_start_time.isoformat()}...")

    telegram_client_instance: Optional[TelegramClient] = None
    # Telegram client setup
    try:
        api_id_str = os.environ.get("API_ID")
        api_hash = os.environ.get("API_HASH")
        bot_token = os.environ.get("BOT_TOKEN")
        
        if not all([api_id_str, api_hash, bot_token]):
            logger.warning("Telegram API credentials (API_ID, API_HASH, BOT_TOKEN) not fully set in .env file. Telegram notifications will be disabled.")
        else:
            api_id = int(api_id_str)
            telegram_client_instance = TelegramClient('bot_session', api_id, api_hash)
            await telegram_client_instance.start(bot_token=bot_token)
            # A simple check to see if the client is authorized and connected.
            if await telegram_client_instance.is_user_authorized():
                logger.info("Telegram client started and authorized successfully.")
            else:
                logger.error("Telegram client failed to authorize. Check credentials or session.")
                await telegram_client_instance.disconnect()
                telegram_client_instance = None

    except ValueError:
        logger.error("Invalid API_ID for Telegram. Please ensure it's a valid integer.")
        telegram_client_instance = None
    except FloodWaitError as e:
        logger.error(f"Telegram FloodWaitError during client start: {e.seconds} seconds. Please wait or retry.")
        telegram_client_instance = None
    except Exception as e:
        logger.error(f"Failed to start Telegram client: {e}. Telegram notifications will be disabled.")
        telegram_client_instance = None

    # API Fetching
    # Generate search queries: all two-letter combinations of lowercase letters
    search_queries = [f"{c1}{c2}" for c1 in string.ascii_lowercase for c2 in string.ascii_lowercase]
    # Optionally add digits if your API handles them as search queries
    # search_queries.extend([str(d) for d in range(10)])

    if not API_URL:
        logger.error("API_URL is not set. Cannot fetch drug data.")
        return

    all_raw_drugs = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS) # Semaphore to limit concurrent API requests

    try:
        async with aiohttp.ClientSession() as session:
            # Process all search queries in batches
            for i in range(0, len(search_queries), BATCH_SIZE):
                batch_queries = search_queries[i:i+BATCH_SIZE]
                logger.info(f"Starting API batch {i//BATCH_SIZE + 1}/{len(search_queries)//BATCH_SIZE + (len(search_queries)%BATCH_SIZE > 0)}. Processing {len(batch_queries)} queries...")

                tasks = [fetch_drug_data_for_query(session, q, semaphore) for q in batch_queries]
                results = await asyncio.gather(*tasks)
                
                for _, drugs_from_batch in results:
                    if drugs_from_batch:
                        all_raw_drugs.extend(drugs_from_batch)
                
                # Add a delay between batches to be polite to the API, especially if MAX_CONCURRENT_REQUESTS is high
                # or if you are hitting rate limits. Adjust this delay as needed.
                if i + BATCH_SIZE < len(search_queries):
                    sleep_duration = 5 # Seconds
                    logger.info(f"Batch {i//BATCH_SIZE + 1} completed. Sleeping for {sleep_duration}s to avoid API rate limits...")
                    await asyncio.sleep(sleep_duration)
            
            logger.info(f"Finished fetching data from API. Found {len(all_raw_drugs)} raw drug records.")

            if all_raw_drugs:
                # Map API records to internal structure and remove duplicates by 'ID'
                mapped_drugs = [map_api_record_to_internal(d) for d in all_raw_drugs if d.get("ID")]
                
                # Create a dictionary to hold unique drugs, keyed by 'ID'
                unique_drugs_dict = {}
                for d in mapped_drugs:
                    drug_id = d.get("ID")
                    if drug_id and drug_id not in unique_drugs_dict:
                        unique_drugs_dict[drug_id] = d
                
                unique_drugs_list = list(unique_drugs_dict.values())
                logger.info(f"Processed {len(mapped_drugs)} records, resulting in {len(unique_drugs_list)} unique drugs.")

                # Upload processed drugs to Supabase history
                await upload_to_history_async(unique_drugs_list)
            else:
                logger.info("No drug data was fetched from the API.")
            
            # After processing API data and uploading to history, check for notifications
            await compare_history_and_notify(script_start_time, telegram_client_instance)

    except Exception as e:
        logger.exception(f"An unhandled error occurred in the main execution loop: {e}")
    finally:
        if telegram_client_instance and telegram_client_instance.is_connected():
            await telegram_client_instance.disconnect()
            logger.info("Telegram client disconnected.")
        logger.info("Script finished execution.")

if __name__ == "__main__":
    # Set Windows ProactorEventLoopPolicy for Windows compatibility if needed
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
