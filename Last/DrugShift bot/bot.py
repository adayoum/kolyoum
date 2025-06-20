from telethon import TelegramClient, events
import os
from dotenv import load_dotenv
import sys
import tempfile
from telethon.sync import TelegramClient as SyncTelegramClient # Not strictly needed if only using async

# Load environment variables
load_dotenv()

# Bot configuration
BOT_TOKEN = os.getenv('BOT_TOKEN')
TARGET_CHANNEL = int(os.getenv('TARGET_CHANNEL', '-1002597303646'))

# variable client will be created only when needed
client = None

def get_client() -> TelegramClient:
    global client
    if client is None:
        # Use a temporary session file if no specific session name is critical
        # Otherwise, ensure 'bot_session' path is writable
        client = TelegramClient('bot_session', api_id=9669209, api_hash='c5338ada2e9ed35cd0b9365c210cb3d9')
    return client

# Async function to send a message to the channel from any other Python code
async def send_message_to_channel(message: str, client: None = None):
    """
    Send a message to the target channel asynchronously (can be called from any async Python code).
    Reuses the running Telethon client session if available, or uses the global client if not provided.
    """
    import logging
    logger = logging.getLogger("bot_send")
    logger.info(f"[send_message_to_channel] BOT_TOKEN: {BOT_TOKEN}")
    logger.info(f"[send_message_to_channel] TARGET_CHANNEL: {TARGET_CHANNEL}")

    if not TARGET_CHANNEL:
        logger.error('Please set TARGET_CHANNEL in your .env file!')
        raise ValueError('Please set TARGET_CHANNEL in your .env file!')

    tg_client = client if client else get_client()

    try:
        # ONLY START THE CLIENT IF IT'S NOT ALREADY CONNECTED
        if not tg_client.is_connected():
            await tg_client.start(bot_token=BOT_TOKEN)
            logger.info("[send_message_to_channel] Client started.")
        else:
            logger.info("[send_message_to_channel] Client already connected, reusing existing connection.")

        await tg_client.send_message(entity=TARGET_CHANNEL, message=message)
        logger.info("[send_message_to_channel] Message sent successfully.")
    except Exception as e:
        logger.error(f"[send_message_to_channel] Failed to send message: {e}")
        raise # Re-raise to propagate the error if needed

# Event handlers should only be registered when run as main
if __name__ == '__main__':
    # This block is for when bot.py is run directly, not when imported.
    # It sets up event handlers for interactive bot commands.
    tg_client = get_client()
    tg_client.start(bot_token=BOT_TOKEN) # Start client for interactive bot
    
    @tg_client.on(events.NewMessage(pattern='/start'))
    async def start_handler(event):
        await event.respond('مرحباً! أنا بوت بسيط. أرسل لي أي رسالة وسأرد عليك.\n\nلإرسال رسالة إلى القناة، استخدم الأمر:\n/send رسالتك هنا')

    @tg_client.on(events.NewMessage(pattern='/getid'))
    async def get_channel_id(event):
        await event.respond(f'Current channel ID: {TARGET_CHANNEL}')

    @tg_client.on(events.NewMessage(pattern='/send'))
    async def send_to_channel_command(event): # Renamed to avoid conflict
        if not TARGET_CHANNEL:
            await event.respond('Please set TARGET_CHANNEL in your .env file first!')
            return
        message = event.text.replace('/send', '').strip()
        if not message:
            await event.respond('الرجاء كتابة رسالة بعد الأمر /send')
            return
        try:
            await tg_client.send_message(entity=TARGET_CHANNEL, message=message)
            await event.respond('تم إرسال الرسالة بنجاح! ✅')
        except Exception as e:
            await event.respond(f'حدث خطأ أثناء إرسال الرسالة: {str(e)}')

    @tg_client.on(events.NewMessage)
    async def message_handler(event):
        if event.text.startswith('/'):
            return
        await event.respond('شكراً على رسالتك! هذه استجابة ثابتة من البوت.')

    def main():
        print('Bot is running...')
        tg_client.run_until_disconnected()

    main()
