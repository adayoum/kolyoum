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

def create_notification_image(data: dict, logo_path: str = 'background.png', output_path: str = 'notification.png'):
    """
    ينشئ صورة إشعار احترافية وجذابة مع تصميم متقدم وتأثيرات بصرية حديثة
    """
    from PIL import Image, ImageDraw, ImageFont, ImageEnhance, ImageFilter
    import os
    import math
    
    # إعدادات الصورة
    width, height = 1200, 800  # حجم أكبر للحصول على جودة أفضل
    base_path = os.path.dirname(os.path.abspath(__file__))
    
    # تحميل الخلفية وإنشاء طبقات متعددة
    try:
        full_logo_path = os.path.join(base_path, 'background.png')
        background = Image.open(full_logo_path).convert('RGBA')
        if background.size != (width, height):
            background = background.resize((width, height), Image.LANCZOS)
        
        # إنشاء طبقة تدرج لونية عصرية
        gradient = Image.new('RGBA', (width, height), (0, 0, 0, 0))
        gradient_draw = ImageDraw.Draw(gradient)
        
        # تدرج من الأزرق الداكن إلى الأزرق الفاتح
        for y in range(height):
            alpha = int(120 * (1 - y / height))  # تدرج الشفافية
            color = (15, 25, 35, alpha)  # لون أزرق داكن متدرج
            gradient_draw.line([(0, y), (width, y)], fill=color)
        
        # دمج الخلفية مع التدرج
        img = Image.alpha_composite(background, gradient)
        
        # إضافة تأثير ضبابي خفيف للخلفية
        blurred_bg = img.filter(ImageFilter.GaussianBlur(radius=1))
        img = Image.blend(img, blurred_bg, 0.3)
        
        logger.info(f"تم تحميل الخلفية بنجاح: {full_logo_path}")
    except Exception as e:
        logger.error(f"خطأ في تحميل الخلفية: {e}")
        # إنشاء خلفية متدرجة احترافية
        img = Image.new('RGBA', (width, height), (0, 0, 0, 255))
        draw_bg = ImageDraw.Draw(img)
        for y in range(height):
            ratio = y / height
            r = int(15 + (25 * ratio))
            g = int(25 + (35 * ratio))
            b = int(35 + (45 * ratio))
            draw_bg.line([(0, y), (width, y)], fill=(r, g, b, 255))
    
    draw = ImageDraw.Draw(img)
    
    # تحميل الخطوط مع أحجام متنوعة
    fonts = {}
    font_sizes = {
        'title': 64, 'subtitle': 32, 'price': 120, 'old_price': 28,
        'percent': 36, 'details': 20, 'footer': 16, 'logo': 40
    }
    
    for font_type, size in font_sizes.items():
        try:
            if font_type == 'price':
                fonts[font_type] = ImageFont.truetype(os.path.join(base_path, 'Almarai-ExtraBold.ttf'), size)
            elif font_type in ['title', 'logo']:
                fonts[font_type] = ImageFont.truetype(os.path.join(base_path, 'Almarai-Bold.ttf'), size)
            else:
                fonts[font_type] = ImageFont.truetype(os.path.join(base_path, 'Almarai-Regular.ttf'), size)
        except Exception:
            fonts[font_type] = ImageFont.load_default()
    
    # ألوان عصرية ومتناسقة
    colors = {
        'primary_text': (255, 255, 255, 255),
        'secondary_text': (220, 220, 220, 255),
        'accent_text': (160, 160, 160, 255),
        'price_bg': (255, 59, 48, 255),
        'price_text': (255, 255, 255, 255),
        'old_price': (180, 180, 180, 255),
        'increase': (52, 199, 89, 255),
        'decrease': (255, 59, 48, 255),
        'neutral': (255, 204, 0, 255),
        'card_bg': (255, 255, 255, 25),
        'shadow': (0, 0, 0, 100),
        'glow': (255, 255, 255, 80)
    }
    
    # دالة لرسم نص مع تأثيرات متقدمة
    def draw_advanced_text(text, position, font, color, anchor='la', 
                          shadow=True, glow=False, outline=False, max_width=None):
        x, y = position
        
        # تقليص النص إذا كان طويلاً
        if max_width and draw.textlength(text, font=font) > max_width:
            while draw.textlength(text + "...", font=font) > max_width and len(text) > 10:
                text = text[:-1]
            text += "..."
        
        # رسم الإطار الخارجي
        if outline:
            for dx in [-2, -1, 0, 1, 2]:
                for dy in [-2, -1, 0, 1, 2]:
                    if dx != 0 or dy != 0:
                        draw.text((x+dx, y+dy), text, font=font, fill=colors['shadow'], anchor=anchor)
        
        # رسم التوهج
        if glow:
            for radius in range(8, 0, -2):
                alpha = int(colors['glow'][3] * (radius / 8))
                glow_color = (*colors['glow'][:3], alpha)
                for angle in range(0, 360, 45):
                    gx = x + radius * math.cos(math.radians(angle))
                    gy = y + radius * math.sin(math.radians(angle))
                    draw.text((gx, gy), text, font=font, fill=glow_color, anchor=anchor)
        
        # رسم الظل
        if shadow:
            draw.text((x+3, y+3), text, font=font, fill=colors['shadow'], anchor=anchor)
        
        # رسم النص الأساسي
        draw.text((x, y), text, font=font, fill=color, anchor=anchor)
    
    # دالة لرسم بطاقة مع خلفية شفافة
    def draw_card(x, y, w, h, radius=15):
        # رسم الظل
        shadow_offset = 5
        draw.rounded_rectangle(
            [x+shadow_offset, y+shadow_offset, x+w+shadow_offset, y+h+shadow_offset],
            radius=radius, fill=colors['shadow']
        )
        # رسم البطاقة
        draw.rounded_rectangle(
            [x, y, x+w, y+h],
            radius=radius, fill=colors['card_bg']
        )
    
    # الهوامش والمسافات
    margin = 60
    card_padding = 30
    
    # رسم شعار DrugShift في الأعلى
    logo_y = margin
    draw_advanced_text("DrugShift", (width//2, logo_y), fonts['logo'], 
                      colors['secondary_text'], anchor='ma', glow=True)
    
    # رسم خط فاصل تحت الشعار
    line_y = logo_y + 60
    draw.line([(margin*2, line_y), (width-margin*2, line_y)], 
              fill=colors['accent_text'], width=2)
    
    # بطاقة اسم الدواء
    name_card_y = line_y + 30
    name_card_height = 120
    draw_card(margin, name_card_y, width-2*margin, name_card_height)
    
    # اسم الدواء العربي (عنوان رئيسي)
    name_ar = data['name_ar']
    name_y = name_card_y + card_padding
    draw_advanced_text(name_ar, (width//2, name_y), fonts['title'], 
                      colors['primary_text'], anchor='ma', shadow=True, 
                      max_width=width-2*margin-2*card_padding)
    
    # اسم الدواء الإنجليزي (عنوان فرعي)
    name_en = data['name_en']
    if name_en and name_en.strip() and name_en.lower() != 'name not available':
        name_en_y = name_y + 50
        draw_advanced_text(name_en, (width//2, name_en_y), fonts['subtitle'], 
                          colors['secondary_text'], anchor='ma', 
                          max_width=width-2*margin-2*card_padding)
    
    # بطاقة السعر الرئيسية
    price_card_y = name_card_y + name_card_height + 40
    price_card_height = 180
    draw_card(margin, price_card_y, width-2*margin, price_card_height)
    
    # السعر الجديد مع خلفية ملونة
    new_price = data['new_price']
    percent = data.get('percent', '')
    is_increase = percent.startswith('+')
    is_decrease = percent.startswith('-')
    
    # تحديد لون السعر حسب التغيير
    if is_increase:
        price_color = colors['increase']
        arrow = "📈"
    elif is_decrease:
        price_color = colors['decrease']
        arrow = "📉"
    else:
        price_color = colors['neutral']
        arrow = "💰"
    
    # رسم خلفية السعر
    price_bg_y = price_card_y + 20
    price_bg_height = 80
    draw.rounded_rectangle(
        [margin + card_padding, price_bg_y, width - margin - card_padding, price_bg_y + price_bg_height],
        radius=40, fill=price_color
    )
    
    # رسم السعر الجديد
    price_text = f"{new_price} ج.م {arrow}"
    price_y = price_bg_y + price_bg_height//2
    draw_advanced_text(price_text, (width//2, price_y), fonts['price'], 
                      colors['price_text'], anchor='ma', shadow=True, glow=True)
    
    # معلومات السعر السابق والنسبة
    old_price_y = price_y + 60
    old_price_text = f"السعر السابق: {data['old_price']} ج.م"
    percent_text = f"نسبة التغيير: {data['percent']}"
    
    # رسم السعر السابق
    draw_advanced_text(old_price_text, (width//4, old_price_y), fonts['old_price'], 
                      colors['old_price'], anchor='ma')
    
    # رسم النسبة بلون مناسب
    percent_color = colors['increase'] if is_increase else colors['decrease'] if is_decrease else colors['neutral']
    draw_advanced_text(percent_text, (3*width//4, old_price_y), fonts['percent'], 
                      percent_color, anchor='ma', shadow=True)
    
    # بطاقة التفاصيل
    details_card_y = price_card_y + price_card_height + 30
    details_card_height = 100
    draw_card(margin, details_card_y, width-2*margin, details_card_height)
    
    # تفاصيل إضافية
    details_y = details_card_y + card_padding
    barcode_text = f"الباركود: {data.get('barcode', 'غير متوفر')}"
    timestamp_text = f"آخر تحديث: {data['timestamp']}"
    
    draw_advanced_text(barcode_text, (width//2, details_y), fonts['details'], 
                      colors['accent_text'], anchor='ma')
    draw_advanced_text(timestamp_text, (width//2, details_y + 30), fonts['details'], 
                      colors['accent_text'], anchor='ma')
    
    # رسم حدود زخرفية في الزوايا
    corner_size = 30
    corner_width = 4
    corner_color = colors['secondary_text']
    
    # الزاوية العلوية اليسرى
    draw.arc([margin, margin, margin + corner_size, margin + corner_size], 
             180, 270, fill=corner_color, width=corner_width)
    # الزاوية العلوية اليمنى
    draw.arc([width - margin - corner_size, margin, width - margin, margin + corner_size], 
             270, 360, fill=corner_color, width=corner_width)
    # الزاوية السفلية اليسرى
    draw.arc([margin, height - margin - corner_size, margin + corner_size, height - margin], 
             90, 180, fill=corner_color, width=corner_width)
    # الزاوية السفلية اليمنى
    draw.arc([width - margin - corner_size, height - margin - corner_size, width - margin, height - margin], 
             0, 90, fill=corner_color, width=corner_width)
    
    # إضافة تأثير النعومة النهائي
    img = img.convert('RGB')
    
    # تطبيق تحسين الألوان
    enhancer = ImageEnhance.Color(img)
    img = enhancer.enhance(1.2)  # زيادة تشبع الألوان
    
    enhancer = ImageEnhance.Contrast(img)
    img = enhancer.enhance(1.1)  # زيادة التباين قليلاً
    
    # حفظ الصورة بجودة عالية
    img.save(output_path, 'PNG', quality=98, optimize=True, dpi=(300, 300))
    
    logger.info(f"تم إنشاء صورة الإشعار بنجاح: {output_path}")
    logger.info(f"أبعاد الصورة: {width}x{height} بكسل")
    
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
                        # إرسال رسالة نصية فقط بدلاً من الصورة
                        message = create_notification_message(image_data)
                        notification_sent = await send_telegram_message(message, telegram_client)
                    except Exception as e:
                        logger.error(f"Error creating or sending notification message for ID {drug_data['ID']}: {e}")
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
                logger.critical(f"CRITICAL ERROR: DB Upload failed for batch {i//BATCH_INSERT_SIZE+1}. Error: {e}")

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

def create_notification_message(data: dict) -> str:
    """
    ينشئ رسالة نصية احترافية وجذابة لإشعار تغير سعر الدواء مع تنسيق بصري متقدم.
    """
    name_ar = data.get('name_ar', 'اسم غير متوفر')
    name_en = data.get('name_en', '')
    new_price = data.get('new_price', 'N/A')
    old_price = data.get('old_price', 'N/A')
    percent = data.get('percent', 'N/A')
    barcode = data.get('barcode', 'غير متوفر')
    timestamp = data.get('timestamp', '')

    # تحديد السهم واللون حسب نسبة التغيير
    is_increase = percent.startswith('+')
    is_decrease = percent.startswith('-')
    arrow = '⬆️' if is_increase else ('⬇️' if is_decrease else '➡️')
    percent_color = '🟢' if is_increase else '🔴' if is_decrease else '🟡'
    percent_html = f"<b><span>{percent_color} {percent}</span></b>"

    # إبراز السعر الجديد بخلفية (عبر كود HTML)
    new_price_html = f"<b><u><span style='background-color:#22223b;color:#f2e9e4;padding:2px 8px;border-radius:6px;'>{new_price} ج.م {arrow}</span></u></b>"
    # شطب السعر السابق
    old_price_html = f"<s>{old_price} ج.م</s>"

    # اسم الدواء مع إبراز ورمز
    name_ar_html = f"<b><span style='color:#c9184a;font-size:20px;'>💊 {name_ar}</span></b>"
    name_en_html = f"<i><span style='color:#adb5bd;font-size:13px;'>{name_en}</span></i>" if name_en and name_en.lower() != 'name not available' else ''

    # خط فاصل
    separator = '<b><span style="color:#adb5bd;">——————————————</span></b>'

    # الباركود والتاريخ بخط صغير ورمادي
    details_html = f"<code>الباركود: {barcode}</code>\n<code>آخر تحديث: {timestamp}</code>"

    # بناء الرسالة
    msg = f"""
{name_ar_html}
{name_en_html}
{separator}
<b>السعر الجديد:</b> {new_price_html}
<b>السعر السابق:</b> {old_price_html}
<b>نسبة التغيير:</b> {percent_html}
{separator}
{details_html}
    """
    # إزالة أي أسطر فارغة زائدة
    msg = '\n'.join([line for line in msg.splitlines() if line.strip()])
    return msg

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
