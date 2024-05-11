import requests
import pandas as pd

def scrape_url(url):
    while True:
        print(f"Scraping URL: {url}")
        # Send a GET request to the URL
        response = requests.get(url)
        # Parse the JSON response
        data = response.json()
        print(f"Received data: {data}")
        # Extract the required data from the JSON
        movies = data['grid']['list']
        for movie in movies:
            emsId = movie['emsId']
            title = movie['title']
            # Extract the audienceScore and criticsScore
            audienceScore = movie['audienceScore'].get('score', 'N/A')
            criticsScore = movie['criticsScore'].get('score', 'N/A')
            print(f"Extracted emsId: {emsId}, title: {title}, audienceScore: {audienceScore}, criticsScore: {criticsScore}")
            # Create a new dictionary with the title, emsId, audienceScore, and criticsScore
            movie_dict = {'emsId': emsId, 'title': title, 'audienceScore': audienceScore, 'criticsScore': criticsScore}
            # Write the movie to the CSV file
            df = pd.DataFrame([movie_dict])
            df.to_csv('movies.csv', mode='a', header=False, index=False)
            print("Wrote movie to CSV file")
        # Check if there's a next page URL in the JSON and hasNextPage is True
        if data['pageInfo']['hasNextPage']:
            # If there is, repeat the process with the next page URL
            next_url = 'https://www.rottentomatoes.com/napi/browse/movies_at_home/sort:popular?after=' + data['pageInfo']['endCursor']
            print(f"Next URL to scrape: {next_url}")
            url = next_url
        else:
            break

# Start the scraping the first page URL
scrape_url('https://www.rottentomatoes.com/napi/browse/movies_at_home/sort:popular?after=')