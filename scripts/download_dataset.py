#!/usr/bin/env python3
"""
Download `heart.csv` from Kaggle dataset 'fedesoriano/heart-failure-prediction'.

Usage:
    # requires ~/.kaggle/kaggle.json (Kaggle API token)
    python scripts/download_dataset.py      # downloads and unzips into ./data/
    python scripts/download_dataset.py data # custom destination

This script uses the official `kaggle` Python package (install via requirements.txt).
"""

import os
import sys
from kaggle.api.kaggle_api_extended import KaggleApi


def main(dest_dir: str = "data") -> None:
    api = KaggleApi()
    try:
        api.authenticate()
    except Exception as e:
        print("ERROR: Could not authenticate with Kaggle API. Ensure ~/.kaggle/kaggle.json exists and has permission 600.")
        raise

    os.makedirs(dest_dir, exist_ok=True)
    print(f"Downloading dataset 'fedesoriano/heart-failure-prediction' into '{dest_dir}'...")
    try:
        api.dataset_download_files('fedesoriano/heart-failure-prediction', path=dest_dir, unzip=True, quiet=False)
    except Exception as exc:
        print(f"Download failed: {exc}")
        raise

    print("Download complete. Check the data directory for heart.csv")


if __name__ == '__main__':
    dest = sys.argv[1] if len(sys.argv) > 1 else 'data'
    main(dest)
