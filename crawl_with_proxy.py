import json, os, sys, time
from datetime import datetime

from yars.yars import YARS
from yars.utils import display_results, download_image


# YARS is great but I needed more fields from the response and didn't need the comments.
class BetterYARS(YARS):

    # with date criteria
    def search_reddit(self, query, timeframe="none", after=None, before=None):
        url = "https://www.reddit.com/search.json"
        limit = {"year": 7200, "month": 600, "week": 140, "day": 20, "none": 10}[timeframe]
        params = {"q": query, "limit": limit, "sort": "relevance", "type": "link"}

        if timeframe != "none":
            params = {"q": query, "limit": limit, "sort": "relevance", "type": "link", "t" : timeframe}
        return self.handle_search(url, params, after, before)
    
    def scrape_post_details(self, link):
        url = f"{link}.json"
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
        except Exception as e:
            print(e)
            if response.status_code != 200:
                return None

        post_data = response.json()
        if not isinstance(post_data, list) or len(post_data) < 2:
            return None

        main_post = post_data[0]["data"]["children"][0]["data"]

        return {
            "title": main_post["title"], 
            "body": main_post.get("selftext", ""),
            "score": main_post.get("score", 0),
            "num_comments": main_post.get("num_comments", 0),
            "created_utc": main_post.get("created_utc", 0),
            "thumbnail_link": main_post.get("thumbnail", ""),
            "category": main_post.get("category", ""),
            "up_votes": main_post.get("ups", 0),
            "up_ratio": main_post.get("upvote_ratio", 0),
            "subreddit": main_post.get("subreddit", ""),
            "author": main_post.get("author_fullname", "")
        }


# This is to be copied to the YARS directory.
# get proxyies from the file
# not using proxy anymore
# with open('/tmp/YARS/example/proxies.json', 'r') as file:
#     proxies = json.load(file)

## CONSTANTS
# Symbols-related keywords to search in reddit(hardcoded for test)
SYMBOLS = [
    "Oil", "Natrual Gas", "Gold", "Silver",
    "Copper", "Gasoline", "Diesel",
    "Corn", "Coffee", "Sugar"
]
# Search criteria
TIMEFRAME = sys.argv[1]


# the crawler
def meow(symbols): # meow(proxies, symbols):
    time.sleep(3)
    # for every symbol
    symbols_idx = 0 # , proxies_idx = 0, 0
    while symbols_idx < len(symbols):
        final_result = []
        # init: effectively it means one proxy per one keyword
        # Have tried these and failed : # YARS(proxy="http://localhost:7531") # YARS(proxy=proxies[proxies_idx])
        # No proxy for now. let's see if i get banned :(
        miner = BetterYARS()
        # let's try this
        # internally it is set to fetch by the order of relevance, descending and the reddit 'kind'; 'link'
        # https://www.reddit.com/search.json?q=Oil&limit=20&sort=relevance&type=link&after=1672531200&before=1673136000

        # get to searching
        posts = miner.search_reddit(symbols[symbols_idx], TIMEFRAME)
        time.sleep({"year": 60, "month": 10, "week": 3, "day": 1, "none": 1}[TIMEFRAME])
        for post in posts:
            link = post.get("link", "")
            details = miner.scrape_post_details(link + ".json")
            time.sleep(2)
            if isinstance(details, dict):
                scraped = {"link": link, "search_key": symbols[symbols_idx]} | details
                final_result.append(scraped)
            else:
                continue
            
        # save to json part
        filename = f"/tmp/YARS/example/search_result_of_{symbols[symbols_idx]}.json"
        save_to_json(final_result, filename)

        # # this is for response inspection
        # save_to_json(posts, f"/tmp/YARS/example/testjson_of_{symbols[symbols_idx]}.json")

        # iterate symbols
        symbols_idx += 1
        # rotate proxies
        # proxies_idx = (proxies_idx + 1) % len(proxies)
    
# Function to save post data to a JSON file
def save_to_json(data, filename):
    try:
        with open(filename, "w") as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Data successfully saved to {filename}")
    except Exception as e:
        print(f"Error saving data to JSON file: {e}")


# Main execution
if __name__ == "__main__":

    meow(SYMBOLS)