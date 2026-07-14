#!/usr/bin/env bash
#
# Пересоздать Telegram-сессию и УБЕДИТЬСЯ, что она рабочая.
#
# Запускать откуда угодно:
#   bash /Users/Syncopation/Documents/Git-Local/00-core/mcp-servers/telegram/reauth.sh
#
# Осознанно НЕ использует `uv`: его нет в PATH обычного терминала. Именно это сделало
# прошлую инструкцию невыполнимой — и сервер простоял сломанным три месяца, потому что
# восстановиться по ней было физически нельзя. Собственный .venv проекта уже содержит
# telethon и python-dotenv, так что uv не нужен.

set -euo pipefail

# session_string_generator.py открывает .env по ОТНОСИТЕЛЬНОМУ пути — если не встать в
# нужную папку, новый ключ запишется не туда.
cd "$(dirname "$0")"

PY=".venv/bin/python"
if [ ! -x "$PY" ]; then
  echo "❌ Не найден $PWD/$PY — окружение не создано. Скажи Клоду, он починит." >&2
  exit 1
fi

echo
echo "═══════════════════════════════════════════════════════════════"
echo "  Пересоздание Telegram-сессии"
echo "═══════════════════════════════════════════════════════════════"
echo
echo "  Сейчас спросят:"
echo "    1) номер телефона  — в международном формате, например +352..."
echo "    2) код             — придёт в приложение Telegram"
echo "    3) пароль          — только если у тебя включена 2FA"
echo
echo "  ⚠️  В конце спросят «update your .env file? (y/N)» — ОТВЕТЬ  y"
echo "      (по умолчанию там N, и тогда ключ никуда не запишется)"
echo

# Бэкап кладём ВНЕ репозитория: .gitignore покрывает только «.env», так что «.env.bak»
# оказался бы незаигноренным файлом с ключами внутри git-репо — один `git add -A`, и
# секрет в истории.
BACKUP_DIR="$HOME/.cache/telegram-mcp"
mkdir -p "$BACKUP_DIR"
BACKUP="$BACKUP_DIR/env-backup-$(date +%Y%m%d-%H%M%S)"
cp .env "$BACKUP"
chmod 600 "$BACKUP"
echo "  (старый .env сохранён в $BACKUP — ничего не потеряется)"
echo

"$PY" session_string_generator.py

echo
echo "═══════════════════════════════════════════════════════════════"
echo "  Проверяю, что новый ключ действительно живой..."
echo "═══════════════════════════════════════════════════════════════"
echo

"$PY" - <<'PYCHECK'
import asyncio
import logging
import os
import sys

logging.getLogger("telethon").setLevel(logging.ERROR)

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession

load_dotenv(override=True)
session = os.getenv("TELEGRAM_SESSION_STRING")


async def check():
    client = TelegramClient(
        StringSession(session),
        int(os.getenv("TELEGRAM_API_ID")),
        os.getenv("TELEGRAM_API_HASH"),
    )
    await asyncio.wait_for(client.connect(), timeout=30)
    try:
        if not await client.is_user_authorized():
            print("❌ Ключ в .env всё ещё нерабочий.")
            print("   Скорее всего на вопрос про .env ты ответил N вместо y.")
            print("   Просто запусти этот скрипт ещё раз и ответь y.")
            sys.exit(1)
        me = await client.get_me()
        name = " ".join(filter(None, [me.first_name, me.last_name]))
        handle = f" (@{me.username})" if me.username else ""
        print(f"✅ Сессия живая: {name}{handle}")
    finally:
        await client.disconnect()


try:
    asyncio.run(check())
except SystemExit:
    raise
except Exception as e:
    print(f"❌ Проверка не прошла: {type(e).__name__}: {e}")
    print("   Запусти скрипт ещё раз; если повторится — покажи это Клоду.")
    sys.exit(1)
PYCHECK

echo
echo "═══════════════════════════════════════════════════════════════"
echo "  Готово. Осталось одно:"
echo
echo "     ПОЛНОСТЬЮ закрой Cursor (⌘Q) и открой заново."
echo
echo "  Сейчас в памяти крутится старый код со старым ключом —"
echo "  он подхватит новый только после перезапуска."
echo "═══════════════════════════════════════════════════════════════"
echo
