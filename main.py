import os
import sys
import logging
import asyncio
from datetime import datetime
import json
import aiohttp

from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, FloodWaitError

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for troubleshooting joins
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("userbot.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class TelegramUserBot:
    def __init__(self):
        # === HARDCODED CONFIG (as requested - you can change later) ===
        self.api_id = 22892426
        self.api_hash = "3f5a3cbe7b41ce3436db3a1b3a0e3519"
        self.phone = "+2349130380946"
        self.session_name = "userbot_session"
        
        # Optional: Specific groups to monitor (empty = all joined groups)
        target_ids = ""   # e.g. "-1001234567890,-1009876543210"
        self.target_groups = [int(gid.strip()) for gid in target_ids.split(',') if gid.strip()]
        
        # Notification bot
        self.notify_bot_token = "8883709162:AAH4hi8NPjE3ULxGdd3gcXFCjEwDGnosFbM"
        self.notify_chat_id = "8614416084"
        
        # Temporary code for login
        self.verification_code = None
        
        self.client = TelegramClient(self.session_name, self.api_id, self.api_hash)
        self.joined_groups = set()
        
    async def start(self):
        """Start the userbot"""
        logger.info("Starting Telegram UserBot...")
        
        try:
            # Support for verification code via env var (for Railway)
            if self.verification_code:
                code_callback = lambda: self.verification_code
                logger.info("Using verification code from TELEGRAM_CODE env var")
            else:
                code_callback = None  # Will prompt interactively (local only)

            await self.client.start(phone=self.phone, code_callback=code_callback)
            logger.info("Client started successfully")
            
            # Get me
            me = await self.client.get_me()
            logger.info(f"Logged in as: {me.first_name} (@{me.username if me.username else 'No username'})")
            
            # Load joined groups
            await self.load_joined_groups()
            
            # Register event handlers
            self.register_handlers()
            
            logger.info("UserBot is now running and monitoring groups...")
            
            # Keep the bot running
            await self.client.run_until_disconnected()
            
        except SessionPasswordNeededError:
            logger.error("2FA password required. Please set it or handle manually.")
            raise
        except FloodWaitError as e:
            logger.error(f"Flood wait error: Wait {e.seconds} seconds before retrying.")
            await asyncio.sleep(e.seconds + 5)  # Extra buffer
            raise
        except Exception as e:
            logger.error(f"Error starting bot: {e}", exc_info=True)
            raise

    async def load_joined_groups(self):
        """Load groups the user is part of"""
        try:
            dialogs = await self.client.get_dialogs(limit=200)
            for dialog in dialogs:
                if dialog.is_group or (hasattr(dialog.entity, 'megagroup') and dialog.entity.megagroup):
                    self.joined_groups.add(dialog.id)
                    logger.info(f"Monitoring group: {dialog.name} (ID: {dialog.id})")
        except Exception as e:
            logger.warning(f"Could not load all dialogs: {e}")

    def register_handlers(self):
        """Register event handlers"""
        
        @self.client.on(events.ChatAction)
        async def new_member_handler(event):
            try:
                logger.debug(f"📥 ChatAction received: user_joined={getattr(event, 'user_joined', False)}, chat_id={getattr(event, 'chat_id', None)}")
                
                if not getattr(event, 'user_joined', False):
                    return
                
                chat_id = event.chat_id
                # Filter if target groups are specified
                if self.target_groups and chat_id not in self.target_groups:
                    logger.debug(f"Filtered: chat {chat_id} not in targets.")
                    return
                
                # Only monitor joined groups
                if chat_id not in self.joined_groups:
                    logger.debug(f"Filtered: chat {chat_id} not monitored.")
                    return
                
                user = await event.get_user()
                chat = await event.get_chat()
                
                join_time = datetime.now().isoformat()
                
                data = {
                    "timestamp": join_time,
                    "group_id": chat_id,
                    "group_title": getattr(chat, 'title', "Unknown"),
                    "user_id": user.id,
                    "user_username": getattr(user, 'username', None),
                    "user_first_name": getattr(user, 'first_name', None),
                    "user_last_name": getattr(user, 'last_name', None),
                    "is_premium": getattr(user, 'premium', False)
                }
                
                logger.info(f"🎉 NEW MEMBER JOINED: {json.dumps(data, ensure_ascii=False)}")
                
                # Save locally
                await self.save_join_log(data)
                
                # Send rich notification 
                message = f"""
🎉 <b>New Member Joined</b>

👤 User: {data['user_first_name'] or ''} {data['user_last_name'] or ''} (@{data['user_username'] or 'no username'})
🆔 ID: {data['user_id']}
🏠 Group: {data['group_title']}
📅 Joined: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                """.strip()
                
                await self.send_telegram_notification(message)
                
            except Exception as e:
                logger.error(f"❌ Error handling new member: {e}", exc_info=True)

        @self.client.on(events.Raw)
        async def raw_update_handler(update):
            # Additional fallback for member updates if needed
            pass

    async def save_join_log(self, data):
        """Save join data to a JSON log file"""
        try:
            log_file = "joins.log"
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(data, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.error(f"Failed to save log: {e}")

    async def send_telegram_notification(self, text: str):
        """Send formatted notification via Telegram Bot API (like your Discord script)"""
        if not self.notify_bot_token or not self.notify_chat_id:
            logger.warning("Telegram notification bot not configured.")
            return

        url = f"https://api.telegram.org/bot{self.notify_bot_token}/sendMessage"
        payload = {
            "chat_id": self.notify_chat_id,
            "text": text,
            "parse_mode": "HTML"
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        logger.info("✅ Notification sent to Telegram chat.")
                    else:
                        error = await resp.text()
                        logger.error(f"❌ Failed to send notification: {error}")
        except Exception as e:
            logger.error(f"❌ Notification error: {e}")

async def main():
    bot = TelegramUserBot()
    await bot.start()

if __name__ == "__main__":
    asyncio.run(main())
