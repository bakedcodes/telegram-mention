# Telegram UserBot - New Member Monitor

Production-ready Telethon userbot that monitors new member joins in groups you are part of.

## Features
- Real-time detection of new members in supergroups
- Logs to file and console
- Configurable via environment variables
- Handles flood errors gracefully
- Ready for Railway deployment

## Setup

1. **API ID & Hash**: Visit https://my.telegram.org → Log in with your phone → API Development Tools → Create new application. Copy **api_id** and **api_hash**.
2. **Notification Bot**:
   - Message [@BotFather](https://t.me/BotFather) → `/newbot` → Get **BOT_TOKEN**.
   - Get your target **CHAT_ID** (private chat or group ID). You can use @userinfobot or forward a message and check via API.
3. Copy `.env.example` → `.env` and fill **all** variables.
4. Deploy.

## Local Run
```bash
pip install -r requirements.txt
python main.py
```

First run will ask for phone number and code (handled via env if set properly).

## Railway Deployment
- Connect GitHub repo
- Add environment variables: `API_ID`, `API_HASH`, `PHONE_NUMBER`
- Railway will handle the rest
