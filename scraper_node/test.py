import requests
from bs4 import BeautifulSoup
from collections import defaultdict
import math
from common.redis_client_base import RedisClientBase
import redis

# Dictionary to store indexed content: URL -> text content
web_index = defaultdict(str)

class ScraperRedisClient(RedisClientBase):

    def build(self):
        entries = [
            {'URL': 'http://example1.com', 'timestamp': 1633000000, 'assigned_status': 'unassigned'},
            {'URL': 'http://example2.com', 'timestamp': 1633100000, 'assigned_status': 'unassigned'},
            {'URL': 'http://example3.com', 'timestamp': 1633200000, 'assigned_status': 'unassigned'}
        ]

        for data in entries:
            entry_id = self._client.incr('entry_id_counter')  # Auto increment entry ID

            # Store the entry in the hash
            self._client.hset(f'entry:{entry_id}', 'URL', data['URL'])
            self._client.hset(f'entry:{entry_id}', 'timestamp', data['timestamp'])
            self._client.hset(f'entry:{entry_id}', 'assigned_status', data['assigned_status'])

            # Add the entry to the sorted set of unassigned entries
            self._client.zadd('entries_by_timestamp', {f'entry:{entry_id}': data['timestamp']})

    def find_and_assign_oldest_entry(self):
        while True:
            # Get the oldest entries sorted by timestamp
            entries = self._client.zrangebyscore('entries_by_timestamp', '-inf', '+inf')

            for entry_key in entries:
                # Watch the entry to ensure no other client modifies it
                self._client.watch(entry_key)
                print(f"Key : {entry_key}")

                # Check if the entry is still unassigned
                status = self._client.hget(entry_key, 'assigned_status').decode()
                if status == 'unassigned':
                    # Start a transaction
                    pipe = self._client.pipeline()
                    pipe.multi()

                    # Mark the entry as assigned
                    pipe.hset(entry_key, 'assigned_status', 'assigned')

                    # Remove it from unassigned sorted set
                    pipe.zrem('entries_by_timestamp', entry_key)

                    # Add it to assigned sorted set
                    timestamp = int(self._client.hget(entry_key, 'timestamp'))
                    pipe.zadd('assigned_entries', {entry_key: timestamp})

                    try:
                        # Execute the transaction
                        pipe.execute()

                        # If transaction succeeds, return the entry
                        url = self._client.hget(entry_key, 'URL').decode()
                        print(f"Locked and moved entry: {entry_key.decode()} with URL: {url}, Timestamp: {timestamp}")
                        return
                    except redis.WatchError:
                        # If the transaction fails due to modification by another client, retry
                        continue

                # Unwatch the entry if status is not 'unassigned'
                client.unwatch()

            print("No unassigned entries found.")
            return

def connect_to_scraper_redis(host: str, port: int, password: str):
    redis = ScraperRedisClient()

    try:
        redis.connect(host, port, password)

    except Exception as ex:
        print(f"EXCEPTION: {ex}")
        return None

    return redis

def scrape_webpage(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Parse the webpage content
        soup = BeautifulSoup(response.content, 'html.parser')
        text_content = soup.get_text(separator=' ', strip=True)
        
        # Index the content
        web_index[url] = text_content.lower()
        
        return "Successfully scraped and indexed: " + url
    except requests.exceptions.RequestException as e:
        return f"Error scraping {url}: {e}"

def compute_tf(document, term):
    """Compute Term Frequency (TF) of a term in a document."""
    words = document.split()
    term_count = words.count(term)
    return term_count / len(words)

def compute_idf(term, corpus):
    """Compute Inverse Document Frequency (IDF) for a term across the entire corpus."""
    N = len(corpus)  # Total number of documents
    nt = sum(1 for doc in corpus.values() if term in doc.split())  # Documents containing the term
    if nt > 0:
        return math.log(N / nt)
    else:
        return 0

def compute_tfidf(term, document, corpus):
    """Compute TF-IDF score for a term in a document."""
    tf = compute_tf(document, term)
    idf = compute_idf(term, corpus)
    return tf * idf

def search_tfidf(query):
    """Search across indexed documents using TF-IDF ranking."""
    results = defaultdict(float)  # URL -> TF-IDF score

    # Tokenize the query
    search_terms = query.lower().split()

    # Compute TF-IDF for each document
    for url, content in web_index.items():
        for term in search_terms:
            # Sum the TF-IDF score for each search term in each document
            results[url] += compute_tfidf(term, content, web_index)
    
    # Sort results by TF-IDF score in descending order
    sorted_results = sorted(results.items(), key=lambda item: item[1], reverse=True)
    
    # Filter out documents with zero relevance
    sorted_results = [url for url, score in sorted_results if score > 0]
    
    return sorted_results

# Example of scraping multiple pages
urls = [
    "https://startrekfleetcommand.com/fan-art/",
    "https://www.example.com"
]

client = connect_to_scraper_redis("localhost", 6379, "abc123")
#client.build()

if not client.field_exists('entry_id_counter'):
    print("entry_id_counter is not set, setting...")
    client.set_field_value('entry_id_counter', 0)

data1 = {
    'URL': 'http://example.com',
    'timestamp': 100,
    'assigned_status': 'unassigned',
    'node_assignment': 'None'
}

data2 = {
    'URL': 'http://ibm.com',
    'timestamp': 100,
    'assigned_status': 'assigned',
    'node_assignment': '234'
}

# Add entry and automatically increment entry ID
#entry: str = f"entry:{client.increment_field_value('entry_id_counter')}"
#client.set_hash_field_values(entry, data1)

# Add entry and automatically increment entry ID
#entry = f"entry:{client.increment_field_value('entry_id_counter')}"
#client.set_hash_field_values(entry, data2)

client.find_and_assign_oldest_entry()

'''
for url in urls:
    print(scrape_webpage(url))

# Example usage
query = "example"
results = search_tfidf(query)
print("Search results using TF-IDF:", results)
'''
