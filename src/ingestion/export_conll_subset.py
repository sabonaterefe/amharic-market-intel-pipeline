# Commit 1: Export unlabeled CoNLL sample
import pandas as pd
import os
import random

# Configurable paths
INPUT_FILE = "data/processed/all_cleaned_data.csv"
OUTPUT_FILE = "data/labeled/labeled_conll.txt"

def tokenize_amharic(text):
    return text.split()  # Naïve tokenization; replace with smarter tokenizer if needed

def export_sample(n=50):
    if not os.path.exists(INPUT_FILE):
        print("❌ Input data not found.")
        return

    df = pd.read_csv(INPUT_FILE)
    df = df[df["Clean_Message"].notna() & (df["Clean_Message"].str.strip() != "")]
    sample = df.sample(n=min(n, len(df)))

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for message in sample["Clean_Message"]:
            tokens = tokenize_amharic(message)
            for token in tokens:
                f.write(f"{token} O\n")
            f.write("\n") 

    print(f"✅ Exported {len(sample)} messages to: {OUTPUT_FILE}")

if __name__ == "__main__":
    export_sample()
