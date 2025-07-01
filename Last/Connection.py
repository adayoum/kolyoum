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

DB_FIELD_MAPPING = {
    'ID': 'id', 'Commercial Name (English)': 'commercial_name_en', 'Commercial Name (Arabic)': 'commercial_name_ar',
    'Scientific Name/Active Ingredients': 'active_ingredients', 'Manufacturer': 'manufacturer',
    'Current Price': 'current_price', 'Previous Price': 'previous_price',
    'Last Price Update Date': 'last_price_update_date', 'Units': 'units', 'Barcode': 'barcode',
    'Dosage Form': 'dosage_form', 'Uses (Arabic)': 'uses_ar', 'Image URL': 'image_url',
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
    Creates a professional notification image with Arabic support.

    Args:
        data: A dictionary containing the notification text data.
        logo_path: Path to the background logo image.
        output_path: Path to save the generated image.
    """
    width, height = 800, 600
    try:
        background = Image.open(logo_path).convert('RGBA')
        # Resize while maintaining aspect ratio (optional, but good practice)
        background.thumbnail((width, height))
        bg_w, bg_h = background.size
        # Create a new canvas and paste the background in the center
        img = Image.new('RGBA', (width, height), (255, 255, 255, 255))
        img.paste(background, ((width - bg_w) // 2, (height - bg_h) // 2))

    except Exception as e:
        logger.error(f"Could not open background image at {logo_path}: {e}")
        img = Image.new('RGBA', (width, height), (240, 240, 240, 255))

    # --- Create a semi-transparent overlay for text readability ---
    overlay = Image.new('RGBA', img.size, (255, 255, 255, 0))
    draw_overlay = ImageDraw.Draw(overlay)
    rect_margin = 40
    rect_radius = 20
    draw_overlay.rounded_rectangle(
        (rect_margin, rect_margin, width - rect_margin, height - rect_margin),
        radius=rect_radius,
        fill=(255, 255, 255, 235), # White with ~92% opacity
        outline=(200, 200, 200, 150),
        width=2
    )
    img = Image.alpha_composite(img, overlay)
    draw = ImageDraw.Draw(img)

    # --- Font and Color Setup ---
    try:
        font_main_bold = ImageFont.truetype("Arial", 38, encoding='unic')
        font_main_regular = ImageFont.truetype("Arial", 32, encoding='unic')
        font_price = ImageFont.truetype("Arial", 55, encoding='unic')
        font_footer = ImageFont.truetype("Arial", 24, encoding='unic')
    except IOError:
        logger.warning("Arial font not found. Using default font. Arabic text might not render correctly.")
        font_main_bold = ImageFont.load_default()
        font_main_regular = ImageFont.load_default()
        font_price = ImageFont.load_default()
        font_footer = ImageFont.load_default()

    color_title = (20, 40, 80)      # Dark Blue
    color_text = (50, 50, 50)         # Dark Gray
    color_new_price = (217, 48, 37)   # Red
    color_old_price = (110, 110, 110) # Medium Gray
    color_increase = (28, 153, 83)    # Green
    color_decrease = (217, 48, 37)    # Red
    color_footer = (150, 150, 150)    # Light Gray

    # --- Text Drawing Logic ---
    right_margin = 75
    current_y = 70

    def draw_text_right(text: str, y_pos: int, font: ImageFont.FreeTypeFont, fill: Tuple[int, int, int]):
        """Helper to draw right-aligned text."""
        bbox = draw.textbbox((0, 0), text, font=font, anchor='ra')
        text_width = bbox[2] - bbox[0]
        x_pos = width - right_margin
        draw.text((x_pos, y_pos), text, font=font, fill=fill, anchor='ra', align='right')

    # Title
    draw_text_right("تحديث سعر دواء", current_y, font_main_bold, color_title)
    current_y += 60
    draw.line([(right_margin, current_y), (width - right_margin, current_y)], fill=(220, 220, 220), width=2)
    current_y += 30

    # Drug Info
    draw_text_right(f"{data['name_ar']}", current_y, font_main_bold, color_text)
    current_y += 45
    if data.get('name_en'):
        draw_text_right(f"{data['name_en']}", current_y, font_main_regular, color_text)
        current_y += 45
    
    draw_text_right(f"الشكل الدوائي: {data['dosage_form']}", current_y, font_main_regular, color_text)
    current_y += 45
    
    # Price Section
    current_y += 20
    draw_text_right("السعر الجديد", current_y, font_main_regular, color_text)
    current_y += 75
    draw_text_right(f"{data['new_price']} جنيه", current_y, font_price, color_new_price)
    current_y += 50
    
    # Old Price and Percentage
    price_change_text = f"السعر السابق: {data['old_price']} جنيه  |  نسبة التغيير: {data['percent']}"
    percent_color = color_increase if '%' in data['percent'] and data['percent'].startswith('+') else color_decrease
    
    # To color the percentage part differently, we draw it in two parts
    old_price_part = f"السعر السابق: {data['old_price']} جنيه  |  "
    percent_part = f"نسبة التغيير: {data['percent']}"
    
    percent_bbox = draw.textbbox((0,0), percent_part, font=font_main_regular, anchor='ra')
    percent_width = percent_bbox[2] - percent_bbox[0]
    
    draw_text_right(percent_part, current_y, font_main_regular, percent_color)
    draw.text(
        (width - right_margin - percent_width, current_y),
        old_price_part,
        font=font_main_regular,
        fill=color_old_price,
        anchor='ra',
        align='right'
    )
    current_y += 60

    # Footer
    draw.line([(right_margin, current_y), (width - right_margin, current_y)], fill=(220, 220, 220), width=2)
    current_y += 20
    draw_text_right(data['timestamp'], current_y, font_footer, color_footer)

    # --- Save the final image ---
    final_image = img.convert('RGB')
    final_image.save(output_path, "PNG", quality=95, optimize=True)
    logger.info(f"Notification image saved to {output_path}")
    return output_path


async def send_telegram_message(message: str, client: TelegramClient) -> bool:
    target_channel_str = os.environ.get("TARGET_CHANNEL")
    if not target_channel_str:
        logger.warning("TARGET_CHANNEL not set. Cannot send message.")
        return False
    try:
        target_channel = int(target_channel_str) if target_channel_str.lstrip('-').isdigit() else target_channel_str
        await client.send_message(target_channel, message, parse_mode='html')
        logger.info(f"Notification sent successfully to channel {target_channel}.")
        return True
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}", exc_info=True)
        return False

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

# --- NEW COMBINED LOGIC: Reconcile, Notify, and Upload ---
async def process_and_commit_changes(drugs: List[Dict[str, Any]], telegram_client: Optional[TelegramClient]):
    if not supabase: logger.warning("Supabase client not initialized."); return
    if not drugs: return

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
                    for row in resp.data: last_row_by_id[row['id']] = row
            except Exception as e:
                logger.error(f"Error fetching history batch {i//BATCH_RPC_SIZE + 1}: {e}.")
        
        logger.info(f"[Supabase] Retrieved {len(last_row_by_id)} existing history records.")
        
        records_to_commit = []
        for drug_data in drugs:
            drug_id = str(drug_data.get("ID"))
            if not drug_id: continue
            last_db_record = last_row_by_id.get(drug_id)
            if not last_db_record:
                records_to_commit.append({'api_data': drug_data, 'db_data': None, 'is_new': True})
                continue

            api_price = drug_data.get("Current Price")
            db_price = last_db_record.get("current_price")
            if are_values_different(api_price, db_price):
                logger.info(f"[COMPARE] Drug ID {drug_id}: API price = {api_price}, DB price = {db_price} -> DIFF detected")
                records_to_commit.append({'api_data': drug_data, 'db_data': last_db_record, 'is_new': False})
            else:
                logger.info(f"[COMPARE] Drug ID {drug_id}: API price = {api_price}, DB price = {db_price} -> NO change")
        
        if not records_to_commit:
            logger.info("No changes detected. All data is up-to-date."); return
            
        logger.info(f"Found {len(records_to_commit)} records with changes. Processing notifications before commit.")

        final_records_to_upload = []
        notifications_sent = 0
        
        for record in records_to_commit:
            drug_data = record['api_data']
            last_db_record = record['db_data']
            
            if record['is_new']:
                final_records_to_upload.append(drug_data)
                continue

            api_price = drug_data.get("Current Price")
            db_price = last_db_record.get("current_price")
            if are_values_different(api_price, db_price):
                logger.info(f"[NOTIFY] Price change detected for ID {drug_data['ID']}: {db_price} -> {api_price}")
                notification_sent = False
                if telegram_client and telegram_client.is_connected():
                    try:
                        image_data = get_notification_image_data({'previous': last_db_record, 'current': drug_data})
                        image_path = f"notification_{drug_data['ID']}.png"
                        # Use the new improved function to create the image
                        create_notification_image(image_data, logo_path='background.jpg', output_path=image_path)
                        notification_sent = await send_telegram_image(image_path, telegram_client)
                        # Clean up the generated image file
                        if os.path.exists(image_path):
                            os.remove(image_path)
                    except Exception as e:
                        logger.error(f"Error creating or sending notification image for ID {drug_data['ID']}: {e}")
                else:
                    logger.warning(f"Telegram client not available. Cannot send notification for ID {drug_data['ID']}.")
                
                if notification_sent:
                    logger.info(f"Notification for ID {drug_data['ID']} SUCCEEDED. Queuing for DB update.")
                    final_records_to_upload.append(drug_data)
                    notifications_sent += 1
                else:
                    logger.warning(f"Notification for ID {drug_data['ID']} FAILED. Skipping DB update for this run to retry later.")
            else:
                logger.info(f"[SKIP] No price change for ID {drug_data['ID']}.")

        logger.info(f"Processing complete. Total notifications sent: {notifications_sent}. Total records to upload: {len(final_records_to_upload)}.")
        
        if not final_records_to_upload: return

        supabase_records = []
        for record in final_records_to_upload:
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
                logger.info(f"DB Upload: Batch {i//BATCH_INSERT_SIZE+1} ({len(batch)} records) uploaded successfully.")
            except Exception as e:
                logger.critical(f"CRITICAL ERROR: DB Upload failed for batch {i//BATCH_INSERT_SIZE+1}. Some notifications may have been sent without a DB update. Error: {e}")

    except Exception as e:
        logger.exception(f"An unhandled error occurred during process_and_commit_changes: {e}")

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
    timestamp = datetime.datetime.now(cairo_tz).strftime('%d-%m-%Y – %I:%M %p  🇪🇬')

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
        execution_time = time.monotonic() - script_start_time
        logger.info(f"Script finished execution in {execution_time:.2f} seconds.")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    try: asyncio.run(main())
    except KeyboardInterrupt: logger.info("Script interrupted by user.")
    except Exception as e: logger.critical(f"A critical error caused the script to exit: {e}", exc_info=True)
