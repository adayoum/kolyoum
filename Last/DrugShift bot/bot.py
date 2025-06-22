# filename: bot.py

import os
import logging
from telethon import TelegramClient, events
from dotenv import load_dotenv

# --- Configuration and Setup ---

# Load environment variables from .env file
# This should be the first thing to do to ensure all variables are available
load_dotenv()

# Configure logging to provide clear output for monitoring
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Securely Load Credentials and Configuration ---

# Load credentials from environment variables. Using .get() is safer.
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')
TARGET_CHANNEL_STR = os.getenv('TARGET_CHANNEL')
ADMIN_ID_STR = os.getenv('ADMIN_ID')

# --- Pre-run Validation Checks ---

# Validate that all necessary environment variables are set before proceeding.
# This prevents runtime errors due to missing configuration.
missing_vars = []
if not API_ID: missing_vars.append('API_ID')
if not API_HASH: missing_vars.append('API_HASH')
if not BOT_TOKEN: missing_vars.append('BOT_TOKEN')
if not TARGET_CHANNEL_STR: missing_vars.append('TARGET_CHANNEL')
if not ADMIN_ID_STR: missing_vars.append('ADMIN_ID')

if missing_vars:
    error_message = f"CRITICAL ERROR: Missing required environment variables: {', '.join(missing_vars)}. Please check your .env file."
    logger.critical(error_message)
    # Exit the script immediately if configuration is incomplete.
    raise ValueError(error_message)

# Safely convert string variables to integers, with error handling
try:
    TARGET_CHANNEL = int(TARGET_CHANNEL_STR)
    ADMIN_ID = int(ADMIN_ID_STR)
except (ValueError, TypeError):
    error_message = "CRITICAL ERROR: TARGET_CHANNEL and ADMIN_ID must be valid integers in your .env file."
    logger.critical(error_message)
    raise ValueError(error_message)


# --- Initialize the Telegram Client ---

# Initialize the client using a persistent session name.
# The session file ('bot_session.session') will store the bot's authorization.
# Ensure 'bot_session.session' is in your .gitignore file!
client = TelegramClient('bot_session', api_id=int(API_ID), api_hash=API_HASH)


# --- Bot Event Handlers (Commands) ---

@client.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    """
    Handler for the /start command.
    SECURITY: Only the admin can interact with the bot.
    """
    if event.sender_id != ADMIN_ID:
        logger.warning(f"Unauthorized /start attempt from user {event.sender_id}. Ignoring.")
        return  # Silently ignore commands from non-admins

    await event.respond(
        'مرحباً! أنا بوت المراقبة الخاص بك.\n\n'
        '**الأوامر المتاحة:**\n'
        '• `/send <رسالتك>` - لإرسال رسالة إلى القناة.\n'
        '• `/getid` - لمعرفة ID القناة المستهدفة.'
    )

@client.on(events.NewMessage(pattern='/getid'))
async def get_channel_id_handler(event):
    """
    Handler to get the current target channel ID.
    SECURITY: Only the admin can use this command.
    """
    if event.sender_id != ADMIN_ID:
        logger.warning(f"Unauthorized /getid attempt from user {event.sender_id}. Ignoring.")
        return

    await event.respond(f'Current target channel ID is: `{TARGET_CHANNEL}`')

@client.on(events.NewMessage(pattern=r'/send(?: |$)(.*)'))
async def send_to_channel_handler(event):
    """
    Handler for the /send command to broadcast a message to the channel.
    SECURITY: Restricted to the admin user to prevent spam/abuse.
    """
    if event.sender_id != ADMIN_ID:
        logger.warning(f"Unauthorized /send attempt from user {event.sender_id}. Responded with access denied.")
        await event.respond('🚫 **وصول مرفوض!** ليس لديك الصلاحية لاستخدام هذا الأمر.')
        return

    # Extract the message text from the command
    message_text = event.pattern_match.group(1).strip()
    if not message_text:
        await event.respond('يرجى كتابة رسالة بعد الأمر `/send`')
        return

    try:
        await client.send_message(entity=TARGET_CHANNEL, message=message_text)
        await event.respond('✅ تم إرسال الرسالة بنجاح!')
        logger.info(f"Admin ({ADMIN_ID}) successfully sent a message to channel {TARGET_CHANNEL}")
    except Exception as e:
        # SECURITY: Log the detailed error for debugging but show a generic message to the user.
        logger.error(f"Failed to send message via /send command. User: {event.sender_id}. Error: {e}")
        await event.respond('😥 حدث خطأ غير متوقع. لم يتم إرسال الرسالة. يرجى مراجعة السجلات.')

# --- Standalone Function for Other Scripts ---

async def send_notification(message: str):
    """
    A standalone, importable function to send a message to the target channel.
    This can be called from other Python scripts like your scraper.
    """
    if not client.is_connected():
        logger.warning("send_notification was called, but the client was not connected. This function assumes the main bot loop is running.")
        # This function should not be responsible for starting the client.
        # The main loop is. It will just log an error if not connected.
        return
            
    try:
        await client.send_message(entity=TARGET_CHANNEL, message=message)
        logger.info(f"Notification sent successfully to channel {TARGET_CHANNEL}.")
    except Exception as e:
        logger.error(f"Failed to send notification from external script: {e}")


# --- Main Execution Block ---

def main():
    """Main function to start the bot and run it indefinitely."""
    try:
        logger.info("Starting bot...")
        
        # The bot will log in using the provided bot token.
        # It will run until you stop the script (e.g., with Ctrl+C).
        client.start(bot_token=BOT_TOKEN)
        
        logger.info("Bot is now running and listening for messages...")
        client.run_until_disconnected()
        
    except Exception as e:
        logger.critical(f"A fatal error occurred while starting or running the bot: {e}")
    finally:
        logger.info("Bot has been disconnected or stopped.")


if __name__ == '__main__':
    main()
