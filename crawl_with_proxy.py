import json, os, sys, time
from datetime import datetime

from yars.yars import YARS
from yars.utils import display_results, download_image


# This is to be copied to the YARS directory.
# get proxyies from the file
with open('/tmp/YARS/example/proxies.json', 'r') as file:
    proxies = json.load(file)
# Symbols-related keywords to search in reddit(hardcoded for test)
SYMBOLS = [
    "Oil", "Natrual Gas", "Gold", "Silver",
    "Copper", "Gasoline", "Diesel",
    "Corn", "Coffee", "Sugar"
]
# Search criteria
start_date = sys.argv[1]
end_date = sys.argv[2]


# the crawler
def meow(proxies, symbols):
    time.sleep(3)
    # for every symbol
    symbols_idx, proxies_idx = 0, 0
    final_result = []
    while symbols_idx < len(symbols):
        # init: effectively it means one proxy per one keyword
        # Have tried these and failed : # YARS(proxy="http://localhost:7531") # YARS(proxy=proxies[proxies_idx])
        # No proxy for now. let's see if i get banned :(
        miner = YARS() 
        # let's try this
        # https://www.reddit.com/search.json?q=Oil&limit=20&sort=relevance&type=link&after=1672531200&before=1673136000

        # get to searching
        posts = miner.search_reddit(symbols[symbols_idx], limit=20, 
                            after=int(datetime.strptime(start_date, "%Y-%m-%d").timestamp()), 
                            before=int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())
        )
        for post in posts:
            final_result.append({
                "title": post.get("title", ""),
                "author": post.get("author", ""),
                "created_utc": post.get("created_utc", ""),
                "num_comments": post.get("num_comments", 0),
                "score": post.get("score", 0),
                "permalink": post.get("permalink", ""),
                "image_url": post.get("image_url", ""),
                "thumbnail_url": post.get("thumbnail_url", ""),
            })
            
        # save to json part
        filename = f"/tmp/YARS/example/search_result_of_{symbols[symbols_idx]}.json"
        save_to_json(final_result, filename)

        # iterate symbols
        symbols_idx += 1
        # rotate proxies
        proxies_idx = (proxies_idx + 1) % len(proxies)
    
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

    meow(proxies, SYMBOLS)