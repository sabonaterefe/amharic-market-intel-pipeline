# Commit 2: Add Amharic text cleaner
import pandas as pd
import re
import os
from glob import glob

# Clean Amharic-only text
def clean_amharic_text(text):
    if not isinstance(text, str):
        return ""
    text = re.sub(r"[^\u1200-\u137F\u1380-\u139F\u2D80-\u2DDF\s]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

# Find the latest raw data file
def get_latest_scrape_file():
    files = sorted(glob("data/telegram_data_raw_*.xlsx"), reverse=True)
    return files[0] if files else None

def main():
    input_path = get_latest_scrape_file()
    output_path = "data/processed/all_cleaned_data.csv"

    if not input_path:
        print("❌ No Telegram scrape file found in /data/")
        return

    df = pd.read_excel(input_path)
    if "Message" not in df.columns:
        print("❌ 'Message' column not found in input data.")
        return

    df["Clean_Message"] = df["Message"].apply(clean_amharic_text)
    cleaned_df = df[[
        "Channel Title", "Channel Username", "ID", "Clean_Message", "Date", "Media File Name"
    ]]

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    cleaned_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"✅ Processed data saved to: {output_path}")

if __name__ == "__main__":
    main()
