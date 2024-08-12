import asyncio
import aiohttp
import logging
import pandas as pd
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Custom context manager for managing an API session
class APISessionManager:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = None

    async def __aenter__(self):
        logging.info(f"Opening session for {self.base_url}")
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        logging.info(f"Closing session for {self.base_url}")
        await self.session.close()

    async def fetch_data(self, endpoint: str) -> Dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        logging.info(f"Fetching data from {url}")
        async with self.session.get(url) as response:
            data = await response.json()
            return data

# Function to asynchronously fetch data from multiple endpoints
async def fetch_all_data(endpoints: List[str], base_url: str) -> List[Dict[str, Any]]:
    async with APISessionManager(base_url) as api_manager:
        tasks = [api_manager.fetch_data(endpoint) for endpoint in endpoints]
        return await asyncio.gather(*tasks)

# Function to process the fetched data into a pandas DataFrame
def process_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
    logging.info("Processing data into DataFrame")
    # Assuming each dictionary in data corresponds to a row
    df = pd.DataFrame(data)
    return df

# Main function to orchestrate the data fetching and processing
async def main():
    base_url = "https://jsonplaceholder.typicode.com"
    endpoints = ["/posts", "/comments", "/albums", "/photos", "/todos", "/users"]

    # Fetch data from APIs
    data = await fetch_all_data(endpoints, base_url)

    # Flatten the list of lists into a single list
    flattened_data = [item for sublist in data for item in sublist]

    # Process the data into a DataFrame
    df = process_data(flattened_data)

    # Display the DataFrame
    logging.info(f"DataFrame created with shape: {df.shape}")
    print(df.head())

# Run the main function
if __name__ == "__main__":
    asyncio.run(main())
