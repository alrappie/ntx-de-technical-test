#import library
import os
import httpx
import asyncio
from bs4 import BeautifulSoup
import polars as pl
from tqdm.asyncio import tqdm
import json

# Create variables
max_pages = [5, 10, 15, 20, 25]
base_url = 'https://www.fortiguard.com/encyclopedia?type=ips&risk={level}&page={page}'
timeout = 15

#async function
async def fetch_page(session, url):
    try:
        response = await session.get(url, timeout=timeout)
        response.raise_for_status()
        return response.text
    except httpx.HTTPStatusError as e:
        print(f"HTTP Error: {e}")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None
    
async def scrape_level(level, max_pages):
    async with httpx.AsyncClient() as session:
        # Initialize an empty polars DataFrame with the correct schema
        data = pl.DataFrame({'Title': pl.Series(dtype=pl.Utf8), 'Link': pl.Series(dtype=pl.Utf8)})
        skipped_pages = []
        tasks = []

        for page in range(1, max_pages + 1):
            url = base_url.format(level=level, page=page)
            tasks.append(fetch_page(session, url))

        responses = await asyncio.gather(*tasks)

        for page, response in enumerate(responses, start=1):
            if response is None:
                skipped_pages.append(page)
                continue
            soup = BeautifulSoup(response, 'html.parser')
            new_rows = []
            for content in soup.find_all('section', class_='table-body')[0].find_all('div', class_='row'):
                try:
                    link = content.get('onclick').split("'")[1]
                    title = content.find('b').getText()
                    new_rows.append({'Title': title, 'Link': link})
                except Exception as e:
                    print(f"Error parsing content: {e}")

            # Append new rows to the DataFrame
            if new_rows:
                new_data = pl.DataFrame(new_rows)
                data = data.vstack(new_data)

        # Save to CSV
        data.write_csv(f'A:/Pemrograman/Python/Project/DE_NTX_TEST/ntx-de-technical-test/Soal 3 - Data Collection Case/datasets/forti_lists_{level}.csv')

        # Save skipped pages
        if skipped_pages:
            with open('A:/Pemrograman/Python/Project/DE_NTX_TEST/ntx-de-technical-test/Soal 3 - Data Collection Case/datasets/skipped.json', 'a') as f:
                json.dump({f'level_{level}': skipped_pages}, f, indent=4)
                
async def main():
    tasks = []
    for level in range(1, 6):  # 5 levels
        tasks.append(scrape_level(level, max_pages[level - 1]))

    for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
        await future

if __name__ == "__main__":
    asyncio.run(main())