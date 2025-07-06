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
from PIL import Image, ImageDraw, ImageFont, ImageEnhance

# Load environment variables from .env file
# SECURITY: Never commit your .env file to a public repository.
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

def parse_iso_datetime(ts_str: Any) -> Optional[datetime.datetime]:
    if not isinstance(ts_str, str): return None
    try: return datetime.datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
    except (ValueError, TypeError): return None

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
        'Previous Price': None,
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

def create_notification_image(data: Dict[str, Any], logo_path: str = 'background.jpg', output_path: str = 'notification.png'):
    """
    النسخة النهائية: تنشئ صورة احترافية بخلفية الشعار وألوان محسنة.
    """
    width, height = 800, 600
    base_path = os.path.dirname(os.path.abspath(__file__))
    
    try:
        full_logo_path = os.path.join(base_path, logo_path)
        img = Image.open(full_logo_path).convert('RGB').resize((width, height))
        logger.info(f"تم تحميل خلفية الشعار بنجاح من: {full_logo_path}")
    except FileNotFoundError:
        logger.error(f"CRITICAL: لم يتم العثور على ملف الشعار '{logo_path}'.")
        img = Image.new('RGB', (width, height), (255, 255, 255))
    except Exception as e:
        logger.error(f"An error occurred while opening the background image: {e}")
        img = Image.new('RGB', (width, height), (255, 255, 255))

    draw = ImageDraw.Draw(img)

    try:
        font_regular = ImageFont.truetype(os.path.join(base_path, 'Almarai-Regular.ttf'), 36)
        font_bold = ImageFont.truetype(os.path.join(base_path, 'Almarai-Bold.ttf'), 42)
        font_price = ImageFont.truetype(os.path.join(base_path, 'Almarai-ExtraBold.ttf'), 60)
        font_footer = ImageFont.truetype(os.path.join(base_path, 'Almarai-Regular.ttf'), 26)
        logger.info("تم تحميل خط المراعي بنجاح.")
    except IOError:
        logger.critical("خطأ فادح: لم يتم العثور على خطوط المراعي!")
        font_regular, font_bold, font_price, font_footer = (ImageFont.load_default(),) * 4

    color_text_light = (255, 255, 255)
    color_shadow = (0, 0, 0)
    color_new_price = (255, 59, 48)
    color_increase = (0, 200, 83)
    color_decrease = (255, 59, 48)
    
    center_x = width / 2
    current_y = 120
    shadow_offset = 2

    def draw_text_center_with_shadow(text: str, y_pos: int, font: ImageFont.FreeTypeFont, fill_color: Tuple[int, int, int]):
        draw.text((center_x + shadow_offset, y_pos + shadow_offset), text, font=font, fill=color_shadow, anchor='ms', align='center')
        draw.text((center_x, y_pos), text, font=font, fill=fill_color, anchor='ms', align='center')

    draw_text_center_with_shadow(f"{data['name_ar']}", current_y, font_bold, color_text_light)
    current_y += 55
    if data.get('name_en'):
        draw_text_center_with_shadow(f"{data['name_en']}", current_y, font_regular, color_text_light)
        current_y += 50
    
    current_y += 60
    draw_text_center_with_shadow("السعر الجديد", current_y, font_bold, color_text_light)
    current_y += 85
    draw_text_center_with_shadow(f"{data['new_price']} جنيه", current_y, font_price, color_new_price)
    current_y += 65

    percent_color = color_increase if '%' in data['percent'] and data['percent'].startswith('+') else color_decrease
    price_change_text = f"السعر السابق: {data['old_price']} جنيه  |  نسبة التغيير: {data['percent']}"
    draw_text_center_with_shadow(price_change_text, current_y, font_regular, percent_color)
    current_y += 60

    draw_text_center_with_shadow(f"Barcode: {data.get('barcode', 'N/A')}", current_y, font_footer, color_text_light)
    current_y += 35
    draw_text_center_with_shadow(data['timestamp'], current_y, font_footer, color_text_light)

    img.save(output_path, "PNG", quality=95, optimize=True)
    logger.info(f"تم حفظ الصورة بنجاح في '{output_path}'")
    return output_path


async def send_telegram_image(image_path, client, caption=""):
    target_channel_str = os.environ.get("TARGET_CHANNEL")
    if not target_channel_str:
        logger.warning("TARGET_CHANNEL not set. Cannot send image.")
        return False
    try:
        target_channel = int(target_channel_str) if target_channel_str.lstrip('-').isdigit() else target_channel_str
        await client.send_file(target_channel, image_path, caption=caption)
        logger.info(f"Image notification sent successfully to channel {target_channel}.")
        return True
    except Exception as e:
        logger.error(f"Failed to send Telegram image: {e}", exc_info=True)
        return False

def get_notification_image_data(change_info: Dict[str, Any]) -> Dict[str, Any]:
    """Prepares the data dictionary needed for creating the notification image."""
    curr_record = change_info['current']
    prev_record = change_info['previous']
    
    old_price_val = prev_record.get('current_price')
    new_price_val = curr_record.get('Current Price')
    
    try:
        if old_price_val is not None and new_price_val is not None:
            old_p, new_p = Decimal(str(old_price_val)), Decimal(str(new_price_val))
            if old_p > 0:
                percent = ((new_p - old_p) / old_p) * 100
                percent_str = f"{percent:+.2f}%"
            else:
                percent_str = "N/A"
        else:
            percent_str = "N/A"
    except (InvalidOperation, TypeError):
        percent_str = "N/A"

    cairo_tz = datetime.timezone(datetime.timedelta(hours=3))
    timestamp = datetime.datetime.now(cairo_tz).strftime('%d-%m-%Y – %I:%M %p')

    return {
        'name_ar': curr_record.get('Commercial Name (Arabic)', "اسم غير متوفر"),
        'name_en': curr_record.get('Commercial Name (English)', "Name not available"),
        'dosage_form': curr_record.get('Dosage Form', "غير محدد"),
        'barcode': curr_record.get('Barcode', "لا يوجد"),
        'old_price': f"{old_price_val:g}" if old_price_val is not None else "N/A",
        'new_price': f"{new_price_val:g}" if new_price_val is not None else "N/A",
        'percent': percent_str,
        'timestamp': timestamp
    }


# --- MODIFIED: RPC-BASED LOGIC TO SOLVE EGRESS PROBLEM ---
async def process_and_commit_changes(drugs: List[Dict[str, Any]], telegram_client: Optional[TelegramClient]):
    """
    This function uses a Supabase RPC call to find changed drugs, minimizing data transfer (Egress).
    It sends all scraped data to a database function, which then returns only the differences.
    """
    if not supabase: logger.warning("Supabase client not initialized."); return
    if not drugs: return

    logger.info(f"Starting processing for {len(drugs)} unique drugs using RPC.")

    try:
        # 1. Format the scraped data to match the structure of our SQL type
        # This prepares the data to be sent to the PostgreSQL function.
        drugs_for_rpc = []
        for drug in drugs:
            drugs_for_rpc.append({
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
            })

        # 2. Call the RPC function with all drug data in a SINGLE request
        # This is the key step to reduce Egress. All comparison logic is now on the database.
        logger.info(f"Calling 'find_changed_drugs' RPC with {len(drugs_for_rpc)} records...")
        rpc_response = await asyncio.to_thread(
            lambda: supabase.rpc("find_changed_drugs", {"p_drugs": drugs_for_rpc}).execute()
        )
        
        changed_drugs = rpc_response.data
        logger.info(f"RPC call complete. Found {len(changed_drugs)} changed or new drugs.")

        if not changed_drugs:
            logger.info("No changes detected by the database. All data is up-to-date.")
            return

        # 3. Process notifications ONLY for the changed drugs returned by the RPC
        records_to_commit_to_db = []
        for change in changed_drugs:
            if change['change_type'] == 'NEW':
                logger.info(f"Found new drug ID {change['id']}. Queuing for DB commit.")
                records_to_commit_to_db.append(change)
                continue  # No notification for brand new drugs, just add them to DB

            # This is a price update, so we need to send a notification
            logger.info(f"Price change detected for ID {change['id']}: {change['previous_price']} -> {change['current_price']}")
            
            # Prepare data structures needed for the get_notification_image_data function
            image_data_current = {
                'Commercial Name (Arabic)': change.get('commercial_name_ar'),
                'Commercial Name (English)': change.get('commercial_name_en'),
                'Dosage Form': change.get('dosage_form'),
                'Barcode': change.get('barcode'),
                'Current Price': change.get('current_price')
            }
            image_data_previous = {'current_price': change.get('previous_price')}

            notification_sent = False
            if telegram_client and telegram_client.is_connected():
                try:
                    image_data = get_notification_image_data({'previous': image_data_previous, 'current': image_data_current})
                    image_path = f"notification_{change['id']}.png"
                    create_notification_image(image_data, output_path=image_path)
                    notification_sent = await send_telegram_image(image_path, telegram_client)
                    if os.path.exists(image_path): os.remove(image_path)
                except Exception as e:
                    logger.error(f"Error creating/sending notification for ID {change['id']}: {e}")

            # Only add the record for DB commit if notification was successful
            if notification_sent:
                logger.info(f"Notification SUCCEEDED for ID {change['id']}. Queuing for DB commit.")
                records_to_commit_to_db.append(change)
            else:
                logger.warning(f"Notification FAILED for ID {change['id']}. Skipping DB update for this run.")

        # 4. Commit the verified changes to the database
        if not records_to_commit_to_db:
            logger.info("No records to commit after notification processing.")
            return

        db_payload = []
        for record in records_to_commit_to_db:
            # Map the RPC output fields back to the database column names
            db_payload.append({
                "id": record['id'],
                "commercial_name_en": record['commercial_name_en'],
                "commercial_name_ar": record['commercial_name_ar'],
                "active_ingredients": record.get("active_ingredients"),
                "manufacturer": record.get("manufacturer"),
                "current_price": record['current_price'],
                "previous_price": record.get('previous_price'), # This value now comes from the RPC
                "last_price_update_date": record.get("last_price_update_date"),
                "units": record.get("units"),
                "barcode": record.get("barcode"),
                "dosage_form": record.get("dosage_form"),
                "uses_ar": record.get("uses_ar"),
                "image_url": record.get("image_url"),
                "scraped_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
            })

        logger.info(f"Committing {len(db_payload)} records to the database...")
        # Upsert into 'drugs' (current state) and Insert into 'history' for logging
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
            logger.info("Starting Telegram client...")
            await telegram_client_instance.start(bot_token=bot_token)
            logger.info("Telegram client started and authorized successfully.")
        except Exception as e:
            logger.error(f"Failed to start Telegram client: {type(e).__name__}: {e}. Notifications disabled.")
            telegram_client_instance = None
    else:
        logger.warning("Telegram credentials not fully set. Notifications disabled.")

    if not API_URL:
        logger.error("API_URL is not set. Cannot fetch drug data. Exiting."); return

    all_raw_drugs = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    try:
        async with aiohttp.ClientSession() as session:
            search_queries = [f"{c1}{c2}" for c1 in string.ascii_lowercase for c2 in string.ascii_lowercase]
            tasks = [fetch_drug_data_for_query(session, query, semaphore) for query in search_queries]
            logger.info(f"Launching {len(tasks)} tasks to fetch data...")
            results = await asyncio.gather(*tasks)
            for _, drugs_from_batch in results:
                if drugs_from_batch: all_raw_drugs.extend(drugs_from_batch)

        logger.info(f"Finished fetching API data. Found {len(all_raw_drugs)} raw records total.")
        
        if all_raw_drugs:
            mapped_drugs = [map_api_record_to_internal(d) for d in all_raw_drugs if d]
            valid_mapped_drugs = [d for d in mapped_drugs if d]
            unique_drugs_dict = {d['ID']: d for d in valid_mapped_drugs if d.get('ID')}
            unique_drugs_list = list(unique_drugs_dict.values())
            logger.info(f"Processed into {len(unique_drugs_list)} unique drugs.")
            
            if unique_drugs_list:
                await process_and_commit_changes(unique_drugs_list, telegram_client_instance)
        else:
            logger.info("No drug data was fetched from the API across all queries.")
            
    except Exception as e:
        logger.exception(f"An unhandled error occurred in the main execution loop: {e}")
    finally:
        if telegram_client_instance and telegram_client_instance.is_connected():
            await telegram_client_instance.disconnect()
            logger.info("Telegram client disconnected.")
        execution_time = time. monotonic() - script_start_time
        logger.info(f"Script finished execution in {execution_time:.2f} seconds.")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    try: asyncio.run(main())
    except KeyboardInterrupt: logger.info("Script interrupted by user.")
    except Exception as e: logger.critical(f"A critical error caused the script to exit: {e}", exc_info=True)
