import json
import requests
import pandas as pd
import os
import re

FOLDER_PATH = "data"
FILE_EXT = ".csv"
FILE_PREFIX = "a_"
PAGE_COUNT = 100

#cleaning section
def convert_score(row):
    if 'score' in row:
        original_score = row['score']
        
        if pd.isna(original_score):
            return 'NEUTRAL'
        
        if isinstance(original_score, (float, int)):
            # Map the score to a sentiment
            if original_score > 3.5:
                return 'POSITIVE'
            elif 2.5 < original_score <= 3.5:
                return 'NEUTRAL'
            elif original_score <= 2.5:
                return 'NEGATIVE'
    
    return 'NEUTRAL'
#end of cleaning section

def get_folder_name(audience_score, critics_score):
    if audience_score is None or critics_score is None:
        return 'other'
    elif audience_score > 70 and critics_score > 70:
        return 'a_love_c_love'
    elif audience_score < 50 and critics_score < 50:
        return 'a_hate_c_hate'
    elif audience_score > 70 and critics_score < 50:
        return 'a_love_c_hate'
    elif audience_score < 50 and critics_score > 70:
        return 'a_hate_c_love'
    else:
        return 'other'

def load_data_from_json(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data


def fetch_data(url):
    print(f"Fetching data from URL: {url}")
    res = requests.get(url)
    print(f"Status code: {res.status_code}")
    return res.json()


def process_page_info(res):
    print("Checking page information...")
    pageInfo = res.get("pageInfo", {})
    if not pageInfo:
        return False, None
    hasNextPage = pageInfo.get("hasNextPage", False)
    nextPage = pageInfo.get("endCursor")
    return hasNextPage, nextPage


def save_data(data, filename, release_date_streaming,release_date_theaters, genre):
    print(f"Saving data to '{filename}...")
    df_list = []
    for item in data:
        reviews = item.get("reviews", [])
        df = pd.DataFrame(reviews)
        df['score'] = df.apply(convert_score, axis=1)
        df['Release Date (Streaming)'] = release_date_streaming
        df['Release Date (Theaters)'] = release_date_theaters
        df['Genre'] = genre
        df_list.append(df)
    df = pd.concat(df_list, ignore_index=True)
    df = df.rename(columns={
        'creationDate': 'Dates',
        'score': 'Scores',
        'quote': 'Reviews'
    })
    df = df[['Dates', 'Scores', 'Reviews', 'Release Date (Streaming)', 'Release Date (Theaters)', 'Genre']]
    # Ensure the directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    # Check if file exists to avoid writing header multiple times
    write_header = not os.path.exists(filename)

    df.to_csv(filename, mode='a', header=write_header, index=False)

def main():
    print("Fetching data...")
    newdict = load_data_from_json("records.json")
    with open("completed_movies.log", "a") as log_file:
        for movie in newdict:
            emsId = movie['emsId']
            cleaned_movie_title = re.sub(r'\W+', '', movie['title'].lower().replace(' ', '_'))
            initial_url = f"https://www.rottentomatoes.com/napi/movie/{emsId}/reviews/user?after=&pageCount={PAGE_COUNT}"
            initial_res = fetch_data(initial_url)
            hasNextPage, nextPage = process_page_info(initial_res)
            result = [initial_res]
            release_date_streaming = movie.get('Release Date (Streaming)')
            release_date_theaters = movie.get('Release Date (Theaters)')

            folder_name = get_folder_name(movie['audience'], movie['critics'])
            save_data(
                [initial_res], f"{FOLDER_PATH}/{folder_name}/{FILE_PREFIX}{cleaned_movie_title}_{FILE_EXT}", release_date_streaming,release_date_theaters, movie['genres'])

            print(f"Fetching remaining data for movie {movie['title']}...")
            while hasNextPage:
                url = f"https://www.rottentomatoes.com/napi/movie/{emsId}/reviews/user?after={nextPage}&pageCount={PAGE_COUNT}"
                res = fetch_data(url)
                result.append(res)
                save_data(
                    [initial_res], f"{FOLDER_PATH}/{folder_name}/{FILE_PREFIX}{cleaned_movie_title}_{FILE_EXT}", release_date_streaming, release_date_theaters, movie['genres'])
                hasNextPage, nextPage = process_page_info(res)
            
            # Write the completed movie's emsId to the log file
            log_file.write(f"{emsId} completed\n")
    print("Done.")

if __name__ == "__main__":
    main()
