import asyncio
import httpx
import csv
import logging
from datetime import datetime
from pathlib import Path

# URLs to fetch
URLS = [
    "https://raw.githubusercontent.com/anshlambagit/AnshLambaYoutube/refs/heads/main/DBT_Masterclass/dim_customer.csv",
    "https://raw.githubusercontent.com/anshlambagit/AnshLambaYoutube/refs/heads/main/DBT_Masterclass/dim_store.csv",
    "https://raw.githubusercontent.com/anshlambagit/AnshLambaYoutube/refs/heads/main/DBT_Masterclass/dim_date.csv",
    "https://raw.githubusercontent.com/anshlambagit/AnshLambaYoutube/refs/heads/main/DBT_Masterclass/dim_product.csv",
    "https://raw.githubusercontent.com/anshlambagit/AnshLambaYoutube/refs/heads/main/DBT_Masterclass/fact_sales.csv",
    "https://raw.githubusercontent.com/anshlambagit/AnshLambaYoutube/refs/heads/main/DBT_Masterclass/fact_returns.csv"
]

async def fetch_data():
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Create data directory
    dataDir = Path(".claude/skills/fetch-api/data") / timestamp
    dataDir.mkdir(parents=True, exist_ok=True)

    # Create log directory
    logDir = Path(".claude/skills/fetch-api/logs") / timestamp
    logDir.mkdir(parents=True, exist_ok=True)

    # Setup logging
    logFile = logDir / "fetch-api.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(logFile),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)

    logger.info("Starting API data fetch process")
    logger.info(f"Data directory: {dataDir}")
    logger.info(f"Log directory: {logDir}")

    async with httpx.AsyncClient(timeout=30.0) as client:
        for url in URLS:
            try:
                logger.info(f"Fetching: {url}")
                response = await client.get(url)
                response.raise_for_status()

                # Extract filename from URL
                filename = url.split('/')[-1]
                filePath = dataDir / filename

                # Save CSV file
                filePath.write_text(response.text)
                logger.info(f"Successfully saved: {filename}")

            except httpx.HTTPError as e:
                logger.error(f"HTTP error fetching {url}: {str(e)}")
            except Exception as e:
                logger.error(f"Error processing {url}: {str(e)}")

    logger.info("API data fetch process completed")

if __name__ == "__main__":
    asyncio.run(fetch_data())
