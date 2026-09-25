import sys
import logging
import asyncio
from datetime import datetime, timezone
import json
import html
import aiohttp

from telethon import TelegramClient, events, types, utils, functions
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

        # History polling is a fallback for supergroups where Telegram does not
        # deliver a live ChatAction/UpdateChannelParticipant to this account.
        # We track the last message ID per group so we only inspect messages
        # that appeared since the previous scan.
        self.history_poll_interval = 5
        self.history_last_message_id = {}
        self.history_task = None

        # Telegram channel/supergroup update-difference polling. Large
        # supergroups can stop sending passive participant updates to a user
        # session; getChannelDifference is Telegram's supported mechanism for
        # pulling the channel's pending update stream.
        self.channel_poll_interval = 5
        self.channel_pts = {}
        self.channel_entities = {}
        self.channel_task = None

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

            # Start the history fallback after the initial dialog scan. This is
            # deliberately independent of ChatAction/raw updates.
            await self.initialize_history_watermarks()
            self.history_task = asyncio.create_task(self.history_poll_loop())

            # IMPORTANT: for large supergroups, do not rely on passive socket
            # updates alone. Actively poll Telegram's channel update stream.
            await self.initialize_channel_pts()
            await self.inspect_channel_capabilities()
            self.channel_task = asyncio.create_task(self.channel_difference_loop())

            logger.info(
                "BUILD: join-monitor v3 | history fallback + channel difference polling enabled"
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

        # Telegram may explicitly tell a client that a channel has queued
        # updates that must be fetched with getChannelDifference. Telethon
        # normally handles this internally, but log it so this monitor can be
        # audited and the explicit polling path remains visible.
        if isinstance(update, types.UpdateChannelTooLong):
            channel_id = getattr(update, 'channel_id', None)
            try:
                chat_id = utils.get_peer_id(types.PeerChannel(channel_id))
            except Exception:
                chat_id = None
            logger.debug(
                "📡 Telegram UpdateChannelTooLong: channel=%s chat=%s pts=%s",
                channel_id,
                chat_id,
                getattr(update, 'pts', None),
            )
            return

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

    async def initialize_history_watermarks(self):
        """Remember the newest message currently visible in each monitored group.

        This prevents old join service messages from being replayed as new joins
        when the process starts. Future messages are then scanned incrementally.
        """
        logger.info("Initializing message-history watermarks...")
        for chat_id in list(self.joined_groups):
            try:
                if self.target_groups and chat_id not in self.target_groups:
                    continue
                entity = await self.client.get_entity(chat_id)
                latest = await self.client.get_messages(entity, limit=1)
                self.history_last_message_id[chat_id] = latest[0].id if latest else 0
                logger.debug(
                    "History watermark: chat=%s last_message_id=%s",
                    chat_id,
                    self.history_last_message_id[chat_id],
                )
            except Exception as e:
                logger.warning(
                    "Could not initialize history watermark for chat %s: %s",
                    chat_id,
                    e,
                )

    async def history_poll_loop(self):
        """Continuously inspect new service messages as a second detection path.

        Some large/modern supergroups do not produce a usable ChatAction for a
        normal member account. Telegram still defines join service-message
        actions, including joins by invite link and joins approved by an admin.
        Polling the message history gives us a second way to catch those joins.
        """
        logger.info(
            "History fallback enabled (interval=%ss)",
            self.history_poll_interval,
        )

        while self.client.is_connected():
            try:
                for chat_id in list(self.joined_groups):
                    if self.target_groups and chat_id not in self.target_groups:
                        continue
                    try:
                        await self.scan_new_group_messages(chat_id)
                    except FloodWaitError as e:
                        logger.warning(
                            "History scan flood-wait for chat %s: %ss",
                            chat_id,
                            e.seconds,
                        )
                        await asyncio.sleep(min(e.seconds, 30))
                    except Exception as e:
                        logger.warning(
                            "History scan failed for chat %s: %s",
                            chat_id,
                            e,
                            exc_info=True,
                        )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("History polling loop error: %s", e, exc_info=True)

            await asyncio.sleep(self.history_poll_interval)

    async def scan_new_group_messages(self, chat_id):
        """Fetch messages newer than the last watermark and inspect service actions."""
        entity = await self.client.get_entity(chat_id)
        last_id = self.history_last_message_id.get(chat_id, 0)
        newest_seen = last_id
        found = 0

        # iter_messages handles pagination, so a burst of >100 messages cannot
        # silently advance the watermark past unseen service messages.
        async for message in self.client.iter_messages(
            entity,
            min_id=last_id,
            reverse=True,
        ):
            msg_id = getattr(message, 'id', None)
            if msg_id is None:
                continue
            newest_seen = max(newest_seen, msg_id)
            found += 1

            if not isinstance(message, types.MessageService):
                continue

            action = getattr(message, 'action', None)
            if action is None:
                continue

            action_name = type(action).__name__
            logger.debug(
                "📜 History service message: chat=%s msg=%s action=%s",
                chat_id,
                msg_id,
                action_name,
            )

            if isinstance(action, types.MessageActionChatJoinedByLink):
                # In a join-by-link service message, from_id is the joining user.
                sender = getattr(message, 'from_id', None)
                user_id = getattr(sender, 'user_id', None)
                if user_id is None:
                    user_id = getattr(message, 'sender_id', None)

                if user_id:
                    await self._process_join(
                        chat_id,
                        user_id,
                        source='History:ChatJoinedByLink',
                        message=message,
                    )

            elif isinstance(action, types.MessageActionChatAddUser):
                for user_id in (getattr(action, 'users', None) or []):
                    await self._process_join(
                        chat_id,
                        user_id,
                        source='History:ChatAddUser',
                        message=message,
                    )

            elif hasattr(types, 'MessageActionChatJoinedByRequest') and isinstance(
                action, types.MessageActionChatJoinedByRequest
            ):
                # This action has no user_id field; the sender of the service
                # message is the user who was accepted into the group.
                sender = getattr(message, 'from_id', None)
                user_id = getattr(sender, 'user_id', None)
                if user_id is None:
                    user_id = getattr(message, 'sender_id', None)

                if user_id:
                    await self._process_join(
                        chat_id,
                        user_id,
                        source='History:ChatJoinedByRequest',
                        message=message,
                    )

        if newest_seen > last_id:
            self.history_last_message_id[chat_id] = newest_seen
            if found:
                logger.debug(
                    "History scan advanced: chat=%s %s new messages, watermark=%s",
                    chat_id,
                    found,
                    newest_seen,
                )

    async def initialize_channel_pts(self):
        """Load Telegram's per-channel PTS from the dialog state.

        Telegram stores a separate update sequence (PTS) for every
        supergroup/channel. The dialog returned by getDialogs contains this
        state. Keeping it is what lets getChannelDifference ask Telegram for
        only the updates since the last known state.
        """
        logger.info("Initializing channel update state (PTS) for supergroups...")
        self.channel_pts.clear()
        self.channel_entities.clear()

        try:
            async for dialog in self.client.iter_dialogs():
                chat_id = dialog.id
                if chat_id not in self.joined_groups:
                    continue
                entity = dialog.entity
                if not isinstance(entity, types.Channel) or not bool(getattr(entity, 'megagroup', False)):
                    continue

                pts = getattr(getattr(dialog, 'dialog', None), 'pts', None)
                if pts is None:
                    logger.warning(
                        "No channel PTS available for %s (%s); relying on Telethon's normal update handling",
                        dialog.name,
                        chat_id,
                    )
                    continue

                self.channel_pts[chat_id] = int(pts)
                self.channel_entities[chat_id] = entity
                logger.info(
                    "Channel polling enabled: %s (ID=%s, pts=%s)",
                    dialog.name,
                    chat_id,
                    pts,
                )
        except Exception as e:
            logger.error("Failed to initialize channel PTS state: %s", e, exc_info=True)

    async def inspect_channel_capabilities(self):
        """Inspect what Telegram exposes to this account for each supergroup.

        This does not attempt to bypass permissions. It records the server-side
        visibility flags so the log clearly distinguishes a monitoring problem
        from a participant-visibility restriction.
        """
        logger.info("Inspecting Telegram participant visibility for monitored supergroups...")
        for chat_id, entity in list(self.channel_entities.items()):
            try:
                full = await self.client(functions.channels.GetFullChannelRequest(
                    channel=await self.client.get_input_entity(entity)
                ))
                full_chat = getattr(full, 'full_chat', None)
                can_view = bool(getattr(full_chat, 'can_view_participants', False))
                hidden = bool(getattr(full_chat, 'participants_hidden', False))
                count = getattr(full_chat, 'participants_count', None)
                logger.info(
                    "Channel capability: chat=%s name=%s can_view_participants=%s participants_hidden=%s participants_count=%s",
                    chat_id,
                    getattr(entity, 'title', None) or chat_id,
                    can_view,
                    hidden,
                    count,
                )
            except Exception as e:
                logger.warning(
                    "Channel capability check failed: chat=%s error=%s",
                    chat_id,
                    e,
                )

    async def channel_difference_loop(self):
        """Actively pull pending supergroup updates from Telegram.

        This is specifically for large supergroups such as Tangem Chat. The
        Telegram API documents that user sessions may receive fewer passive
        channel updates and that clients can use updates.getChannelDifference
        to retrieve the channel update stream.
        """
        logger.info(
            "Channel difference polling enabled (interval=%ss, channels=%s)",
            self.channel_poll_interval,
            len(self.channel_pts),
        )

        while self.client.is_connected():
            for chat_id in list(self.channel_pts):
                if not self._is_monitored_chat(chat_id):
                    continue
                try:
                    await self.poll_channel_difference(chat_id)
                except FloodWaitError as e:
                    logger.warning(
                        "Channel difference flood-wait: chat=%s wait=%ss",
                        chat_id,
                        e.seconds,
                    )
                    await asyncio.sleep(min(e.seconds, 30))
                except Exception as e:
                    logger.warning(
                        "Channel difference failed for chat=%s: %s",
                        chat_id,
                        e,
                        exc_info=True,
                    )

            await asyncio.sleep(self.channel_poll_interval)

    async def poll_channel_difference(self, chat_id):
        """Fetch and process the channel's pending update difference."""
        pts = self.channel_pts.get(chat_id)
        entity = self.channel_entities.get(chat_id)
        if pts is None or entity is None:
            return

        input_channel = await self.client.get_input_entity(entity)
        result = await self.client(functions.updates.GetChannelDifferenceRequest(
            # Do NOT use force=True here. Telegram documents force=True as a
            # way to skip updates that may be considered unnecessary. For a
            # membership monitor we want the complete update stream, including
            # participant transitions.
            force=False,
            channel=input_channel,
            filter=types.ChannelMessagesFilterEmpty(),
            pts=pts,
            limit=100,
        ))

        result_name = type(result).__name__
        logger.debug(
            "Channel difference: chat=%s pts=%s result=%s",
            chat_id,
            pts,
            result_name,
        )

        # Telegram can tell us the channel's latest state when the supplied
        # PTS is too old. The returned dialog carries the current PTS.
        if isinstance(result, types.updates.ChannelDifferenceTooLong):
            dialog = getattr(result, 'dialog', None)
            new_pts = getattr(dialog, 'pts', None)
            if new_pts is not None:
                self.channel_pts[chat_id] = int(new_pts)
                logger.warning(
                    "Channel difference was too long; reset PTS for chat=%s to %s",
                    chat_id,
                    new_pts,
                )
            return

        new_pts = getattr(result, 'pts', None)
        if new_pts is not None:
            self.channel_pts[chat_id] = int(new_pts)

        for message in (getattr(result, 'new_messages', None) or []):
            await self._handle_channel_difference_message(chat_id, message)

        for update in (getattr(result, 'other_updates', None) or []):
            await self._handle_raw_update(update)

    async def _handle_channel_difference_message(self, chat_id, message):
        """Process service messages returned directly by getChannelDifference."""
        if not isinstance(message, types.MessageService):
            return

        action = getattr(message, 'action', None)
        if action is None:
            return

        logger.debug(
            "📡 Channel difference service message: chat=%s msg=%s action=%s",
            chat_id,
            getattr(message, 'id', None),
            type(action).__name__,
        )

        if isinstance(action, types.MessageActionChatJoinedByLink):
            sender = getattr(message, 'from_id', None)
            user_id = getattr(sender, 'user_id', None)
            if user_id is None:
                user_id = getattr(message, 'sender_id', None)
            if user_id:
                await self._process_join(
                    chat_id,
                    user_id,
                    source='ChannelDifference:ChatJoinedByLink',
                    message=message,
                )
            return

        if isinstance(action, types.MessageActionChatAddUser):
            for user_id in (getattr(action, 'users', None) or []):
                await self._process_join(
                    chat_id,
                    user_id,
                    source='ChannelDifference:ChatAddUser',
                    message=message,
                )
            return

        if hasattr(types, 'MessageActionChatJoinedByRequest') and isinstance(
            action, types.MessageActionChatJoinedByRequest
        ):
            sender = getattr(message, 'from_id', None)
            user_id = getattr(sender, 'user_id', None)
            if user_id is None:
                user_id = getattr(message, 'sender_id', None)
            if user_id:
                await self._process_join(
                    chat_id,
                    user_id,
                    source='ChannelDifference:ChatJoinedByRequest',
                    message=message,
                )

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
