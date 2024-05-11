import csv
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from google.colab import drive
import re

LOCATION='/content/drive'


# Number of threads
num_threads = 10

def process_row(row):
    emsId = row['emsId']  # Assuming the column name is 'emsId'
    url = f"https://www.rottentomatoes.com/m/{emsId}"
    res = requests.get(url)
    soup = BeautifulSoup(res.content, 'html.parser')

    genres = [genre.text for genre in soup.find_all('rt-text', {'slot': 'genre'})]
    pattern = r'Release Date \((.*?)\)</rt-text>.*?<rt-text>(.*?)</rt-text>'

    # Find all matches
    matches = re.findall(pattern, res.text, re.DOTALL)

    # Extract the release dates
    release_date_theaters = None
    release_date_streaming = None
    for match in matches:
        if match[0] == 'Theaters':
            release_date_theaters = match[1]
        elif match[0] == 'Streaming':
            release_date_streaming = match[1]

    return emsId, release_date_streaming, release_date_theaters, ', '.join(genres)

def main():
    drive.mount(LOCATION)
    df = pd.read_csv(f'{LOCATION}/My Drive/movies.csv')
    rows = df.to_dict('records')  # Convert the DataFrame to a list of dicts

    total_rows = len(rows)
    print(f"Total rows to process: {total_rows}")

    # Open the output file
    with open(f'{LOCATION}/My Drive/final.csv', 'w', newline='') as out_file:
        writer = csv.writer(out_file)
        writer.writerow(['emsId', 'Release Date (Streaming)', 'Release Date (Theaters)', 'genres'])  # Write the header row

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = executor.map(process_row, rows)
            for i, result in enumerate(futures, start=1):
                # Write the result to the file
                writer.writerow(result)
                out_file.flush()
                print(f"Processed {i} out of {total_rows} rows ({i/total_rows*100:.2f}%)")

if __name__ == "__main__":
    main()
