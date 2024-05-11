# Rotten Tomatoes Scraping

## Overview

This repository contains a Python script for scraping movie data from Rotten Tomatoes website. The script allows you to retrieve information such as movie titles, ratings, reviews, and more.

## Features

- Scrapes Rotten Tomatoes website for movie data.
- Retrieves information like movie title, rating, reviews, etc.
- Customizable scraping options.
- Saves data to a CSV file for further analysis or processing.

## popularMoviesScraper.py

1. Send a GET request to the provided URL.
2. Parse the JSON response.
3. Extract the required data (emsId, title, audienceScore, and criticsScore) from the JSON.
4. Create a dictionary with the extracted data.
5. Write the dictionary as a row to the `movies.csv` file.
6. Check if there is a next page URL in the JSON response.
7. If a next page URL exists, repeat the process with the new URL.
8. If there are no more pages, exit the loop.

The script starts by calling `scrape_url` with the initial URL: `'https://www.rottentomatoes.com/napi/browse/movies_at_home/sort:popular?after='`.

## rottentomatoAudience_scraper.py

1. Load the initial movie data from the `records.json` file.
2. For each movie, fetch the user reviews from Rotten Tomatoes by sending GET requests to the API.
3. Process the fetched data and create a pandas DataFrame.
4. Clean the review scores and map them to sentiments (Positive, Neutral, or Negative).
5. Create a folder name based on the audience and critics scores for the movie.
6. Save the DataFrame as a CSV file in the corresponding folder, with the filename formatted as `a_<cleaned_movie_title>.csv`.
7. Log the completed movie's emsId in the `completed_movies.log` file.

## rottentomatoCritics_scraper.py

1. Load the initial movie data from the `records.json` file.
2. For each movie, fetch the reviews (critic and user) from Rotten Tomatoes by sending GET requests to the API.
3. For each review, if a review URL is available, scrape the full review text from the corresponding website using BeautifulSoup.
4. Process the fetched data and create a pandas DataFrame.
5. Clean the review scores and map them to sentiments (Positive, Neutral, or Negative).
6. Create a folder name based on the audience and critics scores for the movie.
7. Save the DataFrame as a CSV file in the corresponding folder, with the filename formatted as `c_<cleaned_movie_title>.csv`.
8. Print the progress of the scraping process.

## genreReleaseDateScraper.py

1. Read the initial movie data from the `movies.csv` file using pandas.
2. Define a `process_row` function that takes a single row (dictionary) as input.
3. For each row, the `process_row` function:
   - Constructs the movie page URL based on the `emsId`.
   - Fetches the movie page HTML using `requests`.
   - Parses the HTML using BeautifulSoup.
   - Extracts the genres from the page.
   - Extracts the release dates for theaters and streaming using regular expressions.
4. Create a thread pool with `ThreadPoolExecutor` to process multiple rows concurrently.
5. Map the `process_row` function to each row in the initial data using the thread pool.
6. Write the extracted data (emsId, release dates, genres) to a new `final.csv` file.
7. Print the progress of the data extraction process.
