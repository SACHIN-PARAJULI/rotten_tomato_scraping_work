import json
import requests
import pandas as pd
import os
from bs4 import BeautifulSoup
import requests
from concurrent.futures import ThreadPoolExecutor
import re

FOLDER_PATH = "data"
FILE_EXT = ".csv"
FILE_PREFIX = "c_"
PAGE_COUNT = 50
THREAD_COUNT = 8

#cleaning section
def convert_score(row):
    # Define a mapping from grades to ratios
    grade_to_ratio = {'A+': 1, 'A': 0.85, 'A-': 0.7, 'B+': 0.6, 'B': 0.5, 'B-': 0.4, 'C+': 0.3, 'C': 0.2, 'C-': 0.1, 'D': 0, 'F': 0}
    
    original_score = row.get('originalScore', None)
    sentiment = row.get('scoreSentiment', None)
    
    # Check if both columns are empty
    if pd.isna(original_score) and pd.isna(sentiment):
        return 'NEUTRAL'
    
    # Check if one column contains sentiment and the other is empty or non-numeric
    if (sentiment in ['POSITIVE', 'NEGATIVE'] and pd.isna(original_score)) or (original_score not in grade_to_ratio.keys() and pd.isna(original_score)):
        return sentiment
    
    # Check if one column contains a grade and the other is empty or non-sentiment
    if (original_score in grade_to_ratio.keys() and pd.isna(sentiment)) or (sentiment not in ['POSITIVE', 'NEGATIVE'] and pd.isna(sentiment)):
        return original_score
    
    # Check if one column contains a grade and the other contains a sentiment
    if original_score in grade_to_ratio.keys() and sentiment in ['POSITIVE', 'NEGATIVE']:
        if original_score == 'A+' and sentiment != 'POSITIVE':
            return 'POSITIVE'
        elif original_score == 'A-' and sentiment != 'NEGATIVE':
            return 'NEGATIVE'
        else:
            return sentiment
    
    # Check if one column contains a fraction and the other contains a sentiment
    if '/' in str(original_score) and sentiment in ['POSITIVE', 'NEGATIVE']:

        parts = original_score.split('/')
        if len(parts) != 2:
            return 'NEUTRAL'
        try:
            numerator, denominator = map(float, parts)
        except ValueError:
            return 'NEUTRAL'
        ratio = numerator / denominator
        if 0.4 <= ratio <= 0.6:
            return 'NEUTRAL'
        elif ratio > 0.6:
            return 'POSITIVE'
        else:
            return 'NEGATIVE'
    
    # Default case: return NEUTRAL
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

def scrapeReviewSite(url):
    print(f"Scraping review from {url}")
    review = ""
    review_url = url
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36'}
    try:    
        response = requests.get(review_url, headers=headers, timeout=3)
        print("Received response from website")
        n_html = response.text
        n_soup = BeautifulSoup(n_html, "html.parser")
        for text in n_soup.find_all('p'):
            punct = text.get_text()[-2:]
            if('.' in punct or '!' in punct or '?' in punct):
                if(len(text.get_text().split()) >= 25):
                    review = review + " " + text.get_text()
                    print(f"Added paragraph to review: {text.get_text()[:50]}...")
    except:
        print("Website could not be opened")
        return review
    print("Finished scraping review")
    return review

def process_page_info(res):
    print("Checking page information...")
    pageInfo = res.get("pageInfo", {})
    if not pageInfo:
        return False, None
    hasNextPage = pageInfo.get("hasNextPage", False)
    nextPage = pageInfo.get("endCursor")
    return hasNextPage, nextPage

def process_review(review):
    try:
        review_url = review.get("reviewUrl")
        scraped_review = scrapeReviewSite(review_url)
        if not scraped_review:
            scraped_review = review.get("quote")
        review["Reviews"] = scraped_review
        return review
    except Exception as e:
        print(f"Error processing review: {e}")
        return None
def save_data(data, filename, release_date_streaming, release_date_theaters, genre):
    print(f"Saving data to '{filename}...")
    df_list = []
    for item in data:
        reviews = item.get("reviews", [])
        with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
            reviews = list(executor.map(process_review, reviews))
        df = pd.DataFrame(reviews)
        df['originalScore'] = df.apply(convert_score, axis=1)
        df['Release Date (Streaming)'] = release_date_streaming
        df['Release Date (Theaters)'] = release_date_theaters
        df['Genre'] = genre
        df = df.rename(columns={
            'creationDate': 'Dates',
            'originalScore': 'Scores'
        })
        # Check if the columns exist in the DataFrame
        columns = ['Dates', 'Scores','Reviews', 'Release Date (Streaming)', 'Release Date (Theaters)', 'Genre']
        if all(column in df.columns for column in columns):
            df = df[columns]
        else:
            print(f"Warning: One or more of the columns {columns} do not exist in the DataFrame.")
            df = df[df.columns.intersection(columns)]  # Select only the columns that exist
        df_list.append(df)
    df = pd.concat(df_list, ignore_index=True)
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    # Check if file exists to avoid writing header multiple times
    write_header = not os.path.exists(filename)
    
    # Replace non-UTF-8 characters before saving it to a CSV file
    df = df.apply(lambda x: x.str.encode('utf-8', 'ignore').str.decode('utf-8'))
    df.to_csv(filename, mode='a', header=write_header, index=False, escapechar='\\')

def main():
    print("Fetching data...")
    movies_dict = load_data_from_json("records.json")
    total_movies = len(movies_dict)
    for i, movie in enumerate(movies_dict, start=1):
        emsId = movie['emsId']
        cleaned_movie_title = re.sub(r'\W+', '', movie['title'].lower().replace(' ', '_'))
        initial_url = f"https://www.rottentomatoes.com/napi/movie/{emsId}/reviews/all?after=MA&pageCount={PAGE_COUNT}"
        initial_res = fetch_data(initial_url)
        hasNextPage, nextPage = process_page_info(initial_res)
        result = [initial_res]

        # Fetch both release dates
        release_date_streaming = movie.get('Release Date (Streaming)')
        release_date_theaters = movie.get('Release Date (Theaters)')

        folder_name = get_folder_name(movie['audience'], movie['critics'])
        save_data([initial_res], f"{FOLDER_PATH}/{folder_name}/{FILE_PREFIX}{cleaned_movie_title}_{FILE_EXT}", release_date_streaming, release_date_theaters, movie['genres'])

        print(f"Fetching remaining data for movie {movie['title']}...")
        while hasNextPage:
            url = f"https://www.rottentomatoes.com/napi/movie/{emsId}/reviews/all?after={nextPage}&pageCount={PAGE_COUNT}"
            res = fetch_data(url)
            result.append(res)
            save_data([res], f"{FOLDER_PATH}/{folder_name}/{FILE_PREFIX}{cleaned_movie_title}_{FILE_EXT}", release_date_streaming, release_date_theaters, movie['genres'])
            hasNextPage, nextPage = process_page_info(res)
        
        # Print the progress
        print(f"Processed {i} out of {total_movies} movies ({(i/total_movies)*100:.2f}%).")
    print("Done.")

if __name__ == "__main__":
    main()
