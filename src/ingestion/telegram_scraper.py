# Commit 1: Initial version of async Telegram scraper
from telethon import TelegramClient
from dotenv import load_dotenv
import pandas as pd
import os
import asyncio
import re
from datetime import datetime

# Load credentials
load_dotenv()
api_id = int(os.getenv("TG_API_ID"))
api_hash = os.getenv("TG_API_HASH")
phone = os.getenv("phone")

# Paths
BASE_DIR = "data"
RAW_DIR = os.path.join(BASE_DIR, "raw", "data")
CHANNEL_FILE = os.path.join(RAW_DIR, "channels_to_crawl.xlsx")
PHOTO_DIR = os.path.join(BASE_DIR, "photos")

timestamp = datetime.now().strftime("%Y%m%d_%H%M")
OUTFILE = os.path.join(BASE_DIR, f"telegram_data_raw_{timestamp}.xlsx")
os.makedirs(PHOTO_DIR, exist_ok=True)

# Load channels
df = pd.read_excel(CHANNEL_FILE, header=None)
df.columns = ["channel_username"]
channels = df["channel_username"].dropna().unique().tolist()

client = TelegramClient("scraper_session", api_id, api_hash)

def clean_amharic(text):
    if not isinstance(text, str):
        return ""
    text = re.sub(r"[^\u1200-\u137F\u1380-\u139F\u2D80-\u2DDF\s]", "", text)
    return re.sub(r"\s+", " ", text).strip()

async def scrape_channel(channel):
    try:
        entity = await client.get_entity(channel)
        messages = await client.get_messages(entity, limit=1000)

        print(f"✅ {channel} → {len(messages)} messages")

        short = channel.strip("@")
        rows = []
        for msg in messages:
            text = clean_amharic(msg.message)
            if not text:
                continue

            media_name = f"{short}_{msg.id}.jpg" if msg.media and hasattr(msg.media, "photo") else ""
            rows.append([
                entity.title,
                channel,
                msg.id,
                text,
                msg.date,
                media_name
            ])

        return rows

    except Exception as e:
        print(f"❌ {channel}: {e}")
        return []

async def main():
    await client.start(phone=phone)
    print(f"\n🚀 Scraping {len(channels)} channels in parallel...\n")

    tasks = [scrape_channel(ch) for ch in channels]
    results = await asyncio.gather(*tasks)

    all_rows = [row for result in results for row in result]

    if all_rows:
        df = pd.DataFrame(all_rows, columns=[
            "Channel Title", "Channel Username", "ID", "Message", "Date", "Media File Name"
        ])
        df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)
        df.to_excel(OUTFILE, index=False)
        print(f"\n📦 Scraping complete! Data saved to: {OUTFILE}")
    else:
        print("\n⚠️ No data scraped. Make sure you're joined to the listed channels.")

with client:
    
    client.loop.run_until_complete(main())
