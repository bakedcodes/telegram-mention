import sys
import logging
import asyncio
from datetime import datetime, timezone
import json
import html
import aiohttp

from telethon import TelegramClient, events, types, utils
from telethon.errors import SessionPasswordNeededError, FloodWaitError

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("userbot.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class TelegramUserBot:
    def __init__(self):
        # === HARDCODED CONFIG ===
        self.api_id = 22892426
        self.api_hash = "3f5a3cbe7b41ce3436db3a1b3a0e3519"
        self.phone = "+19705033430"
        self.session_name = "userbot_session"

        # Empty = monitor every group/supergroup the account can see.
        # Example: "-1001234567890,-1009876543210"
        target_ids = ""
        self.target_groups = {
            int(gid.strip()) for gid in target_ids.split(',') if gid.strip()
        }

        # Notification bot
        self.notify_bot_token = "8857382932:AAFI0XH6RZzeT3WsDvi2Q6l0d_dNpOF28p8"
        self.notify_chat_id = "-1004499759470"

        # Optional local/Railway login code support.
        self.verification_code = None

        self.client = TelegramClient(self.session_name, self.api_id, self.api_hash)
        self.joined_groups = set()

        # Prevent duplicate notifications because Telegram/Telethon can expose
        # the same join through more than one update path.
        self.processed_joins = {}
        self.processed_ttl_seconds = 120

    async def start(self):
        """Start the userbot."""
        logger.info("Starting Telegram UserBot...")

        try:
            if self.verification_code:
                code_callback = lambda: self.verification_code
                logger.info("Using configured verification code")
            else:
                code_callback = None

            await self.client.start(phone=self.phone, code_callback=code_callback)
            logger.info("Client started successfully")

            me = await self.client.get_me()
            logger.info(
                "Logged in as: %s (@%s, id=%s)",
                me.first_name,
                me.username if me.username else "No username",
                me.id,
            )

            # Register handlers BEFORE catch_up so updates received during startup
            # are handled by the same logic as live updates.
            self.register_handlers()

            await self.load_joined_groups()

            # Process updates Telegram may have queued while the process was down.
            try:
                await self.client.catch_up()
                logger.info("Initial Telegram update catch-up completed")
            except Exception as e:
                logger.warning("catch_up() failed: %s", e, exc_info=True)

            logger.info(
                "UserBot is now running. Monitoring %d groups/supergroups...",
                len(self.joined_groups),
            )

            await self.client.run_until_disconnected()

        except SessionPasswordNeededError:
            logger.error("2FA password required. Please complete login manually.")
            raise
        except FloodWaitError as e:
            logger.error("Flood wait error: wait %s seconds.", e.seconds)
            await asyncio.sleep(e.seconds + 5)
            raise
        except Exception as e:
            logger.error("Error starting bot: %s", e, exc_info=True)
            raise

    async def load_joined_groups(self):
        """Load ALL visible groups/supergroups, not only the first 200 dialogs."""
        self.joined_groups.clear()
        count = 0

        try:
            async for dialog in self.client.iter_dialogs():
                entity = dialog.entity
                is_group = bool(dialog.is_group)
                is_megagroup = bool(getattr(entity, 'megagroup', False))

                if is_group or is_megagroup:
                    self.joined_groups.add(dialog.id)
                    count += 1
                    logger.info(
                        "Monitoring group: %s (ID: %s, megagroup=%s)",
                        dialog.name,
                        dialog.id,
                        is_megagroup,
                    )

            logger.info("Loaded %d groups/supergroups for monitoring", count)
        except Exception as e:
            logger.warning("Could not load all dialogs: %s", e, exc_info=True)

    def _chat_id_from_peer(self, peer):
        """Convert a Telegram Peer object into Telethon's marked chat ID."""
        if peer is None:
            return None
        try:
            return utils.get_peer_id(peer)
        except Exception:
            return None

    def _message_chat_id(self, message):
        if message is None:
            return None
        try:
            return utils.get_peer_id(message.peer_id)
        except Exception:
            return getattr(message, 'chat_id', None)

    def _is_monitored_chat(self, chat_id):
        if chat_id is None:
            return False
        if self.target_groups and chat_id not in self.target_groups:
            return False
        return chat_id in self.joined_groups

    def _remember_join(self, chat_id, user_id, message_id=None):
        """Return False for a duplicate join notification within the short TTL."""
        now = asyncio.get_running_loop().time()
        key = (chat_id, user_id, message_id)

        # A message ID is normally enough to deduplicate the raw/service paths.
        # For participant updates, message_id is None, so retain those briefly.
        stale = [k for k, t in self.processed_joins.items() if now - t > self.processed_ttl_seconds]
        for k in stale:
            self.processed_joins.pop(k, None)

        if key in self.processed_joins:
            return False
        self.processed_joins[key] = now
        return True

    async def _process_join(self, chat_id, user_id, source, message=None, event=None):
        """Common join processor used by ChatAction and raw Telegram updates."""
        if not self._is_monitored_chat(chat_id):
            logger.debug(
                "Join ignored: chat_id=%s is not in the monitored group set (source=%s)",
                chat_id,
                source,
            )
            return

        if not user_id:
            logger.warning("Join update without a user ID: chat=%s source=%s", chat_id, source)
            return

        message_id = getattr(message, 'id', None) if message is not None else None
        if not self._remember_join(chat_id, user_id, message_id):
            logger.debug(
                "Duplicate join suppressed: chat=%s user=%s source=%s message=%s",
                chat_id, user_id, source, message_id,
            )
            return

        try:
            user = await self.client.get_entity(user_id)
        except Exception as e:
            logger.warning("Could not resolve joined user %s: %s", user_id, e)
            user = None

        try:
            chat = await self.client.get_entity(chat_id)
        except Exception as e:
            logger.warning("Could not resolve group %s: %s", chat_id, e)
            chat = None

        now = datetime.now(timezone.utc)
        data = {
            "timestamp": now.isoformat(),
            "group_id": chat_id,
            "group_title": getattr(chat, 'title', None) or "Unknown",
            "user_id": user_id,
            "user_username": getattr(user, 'username', None),
            "user_first_name": getattr(user, 'first_name', None),
            "user_last_name": getattr(user, 'last_name', None),
            "is_premium": bool(getattr(user, 'premium', False)),
            "detection_source": source,
        }

        logger.info("🎉 NEW MEMBER JOINED: %s", json.dumps(data, ensure_ascii=False))
        await self.save_join_log(data)

        first = html.escape(data['user_first_name'] or '')
        last = html.escape(data['user_last_name'] or '')
        username = html.escape(data['user_username'] or 'no username')
        group_title = html.escape(data['group_title'])

        message_text = (
            "🎉 <b>New Member Joined</b>\n\n"
            f"👤 User: {first} {last} (@{username})\n"
            f"🆔 ID: {data['user_id']}\n"
            f"🏠 Group: {group_title}\n"
            f"📅 Joined: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
            f"🔎 Detection: {html.escape(source)}"
        )

        await self.send_telegram_notification(message_text)

    def register_handlers(self):
        """Register normal and raw update handlers."""

        @self.client.on(events.ChatAction)
        async def new_member_handler(event):
            try:
                chat_id = getattr(event, 'chat_id', None)
                joined = bool(getattr(event, 'user_joined', False))
                added = bool(getattr(event, 'user_added', False))

                logger.debug(
                    "📥 ChatAction: chat=%s joined=%s added=%s users=%s action=%s",
                    chat_id,
                    joined,
                    added,
                    getattr(event, 'user_ids', None),
                    type(getattr(event, 'action_message', None)).__name__,
                )

                # We intentionally handle both user_joined and user_added.
                # A group admin adding another member is still a new member.
                if not (joined or added):
                    return

                users = getattr(event, 'users', None) or []
                if not isinstance(users, (list, tuple)):
                    users = [users]

                for user in users:
                    user_id = getattr(user, 'id', user if isinstance(user, int) else None)
                    await self._process_join(
                        chat_id,
                        user_id,
                        source='ChatAction',
                        message=getattr(event, 'action_message', None),
                        event=event,
                    )

            except Exception as e:
                logger.error("❌ Error handling ChatAction: %s", e, exc_info=True)

        @self.client.on(events.Raw)
        async def raw_update_handler(update):
            """
            Raw fallback.

            Telethon's ChatAction already understands several service-message
            actions, but keeping this handler is useful for updates that do not
            get converted into ChatAction for a particular supergroup/update
            path. In particular, channel/supergroup participant updates can
            contain the join transition directly.
            """
            try:
                await self._handle_raw_update(update)
            except Exception as e:
                logger.error("❌ Raw update handler error: %s", e, exc_info=True)

    async def _handle_raw_update(self, update):
        update_type = type(update).__name__

        # 1) Service messages in small groups and supergroups.
        if isinstance(update, (types.UpdateNewMessage, types.UpdateNewChannelMessage)):
            message = getattr(update, 'message', None)
            if not isinstance(message, types.MessageService):
                return

            action = getattr(message, 'action', None)
            chat_id = self._message_chat_id(message)
            if not chat_id:
                return

            logger.debug(
                "📡 Raw service update: type=%s chat=%s action=%s msg=%s",
                update_type,
                chat_id,
                type(action).__name__ if action else None,
                getattr(message, 'id', None),
            )

            if isinstance(action, types.MessageActionChatJoinedByLink):
                user_id = getattr(message, 'from_id', None)
                user_id = getattr(user_id, 'user_id', None) or getattr(message, 'sender_id', None)
                await self._process_join(
                    chat_id,
                    user_id,
                    source='Raw:ChatJoinedByLink',
                    message=message,
                )
                return

            if isinstance(action, types.MessageActionChatAddUser):
                users = getattr(action, 'users', None) or []
                for user_id in users:
                    await self._process_join(
                        chat_id,
                        user_id,
                        source='Raw:ChatAddUser',
                        message=message,
                    )
                return

        # 2) Channel/supergroup participant state changes.
        # UpdateChannelParticipant is especially useful for modern supergroups
        # where membership can change without the service-message path being
        # delivered as an ordinary ChatAction.
        if isinstance(update, types.UpdateChannelParticipant):
            channel_id = getattr(update, 'channel_id', None)
            chat_id = -1000000000000 - channel_id if channel_id is not None else None

            if not self._is_monitored_chat(chat_id):
                # Resolve the exact marked ID using Telethon's helper when possible.
                try:
                    chat_id = utils.get_peer_id(types.PeerChannel(channel_id))
                except Exception:
                    pass

            user_id = getattr(update, 'user_id', None)
            if hasattr(user_id, 'user_id'):
                user_id = user_id.user_id

            prev = getattr(update, 'prev_participant', None)
            curr = getattr(update, 'new_participant', None)

            logger.debug(
                "📡 Raw participant update: channel=%s chat=%s user=%s prev=%s new=%s",
                channel_id,
                chat_id,
                user_id,
                type(prev).__name__ if prev else None,
                type(curr).__name__ if curr else None,
            )

            if self._participant_is_join(prev, curr):
                await self._process_join(
                    chat_id,
                    user_id,
                    source='Raw:UpdateChannelParticipant',
                )

    @staticmethod
    def _participant_is_join(previous, current):
        """Detect a transition into a member state from a non-member state."""
        if current is None:
            return False

        # Telethon TL participant classes vary slightly by Telegram layer.
        current_name = type(current).__name__
        previous_name = type(previous).__name__ if previous is not None else None

        member_names = {
            'ChannelParticipant',
            'ChannelParticipantSelf',
            'ChannelParticipantAdmin',
            'ChannelParticipantCreator',
            'ChannelParticipantBanned',  # handled below if actually restricted
        }
        non_member_names = {
            None,
            'ChannelParticipantLeft',
            'ChannelParticipantBanned',
        }

        if current_name == 'ChannelParticipantBanned':
            # A banned/restricted participant is not a join.
            return False

        if previous_name in non_member_names and current_name in member_names:
            return True

        # If Telegram sends no previous participant, a current ordinary member
        # participant is still useful as a fallback signal.
        if previous is None and current_name in {
            'ChannelParticipant',
            'ChannelParticipantSelf',
            'ChannelParticipantAdmin',
            'ChannelParticipantCreator',
        }:
            return True

        return False

    async def save_join_log(self, data):
        """Save join data to a JSON-lines log."""
        try:
            with open("joins.log", "a", encoding="utf-8") as f:
                f.write(json.dumps(data, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.error("Failed to save log: %s", e)

    async def send_telegram_notification(self, text):
        """Send formatted notification through the Telegram Bot API."""
        if not self.notify_bot_token or not self.notify_chat_id:
            logger.warning("Telegram notification bot not configured.")
            return

        url = f"https://api.telegram.org/bot{self.notify_bot_token}/sendMessage"
        payload = {
            "chat_id": self.notify_chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        try:
            timeout = aiohttp.ClientTimeout(total=20)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload) as resp:
                    body = await resp.text()
                    if resp.status == 200:
                        logger.info("✅ Notification sent to Telegram chat.")
                    else:
                        logger.error(
                            "❌ Failed to send notification: HTTP %s: %s",
                            resp.status,
                            body,
                        )
        except Exception as e:
            logger.error("❌ Notification error: %s", e, exc_info=True)


async def main():
    bot = TelegramUserBot()
    await bot.start()


if __name__ == "__main__":
    asyncio.run(main())
