import logging
import requests
from bs4 import BeautifulSoup
from collections import defaultdict
import math
from common.scraper_redis_client import ScraperRedisClient

_logger = logging.getLogger(__name__)
log_format= logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                                "%Y-%m-%d %H:%M:%S")
console_stream = logging.StreamHandler()
console_stream.setFormatter(log_format)
_logger.addHandler(console_stream)
_logger.setLevel("INFO")

# Dictionary to store indexed content: URL -> text content
web_index = defaultdict(str)

def connect_to_scraper_redis(host: str, port: int, password: str):
    redis = ScraperRedisClient(_logger)

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
client.initialise_redis(force=False)

client.add_domain_entry("http://test1.com")
client.add_domain_entry("http://test2.com")
"""
client.add_domain_entry("http://test3.com")
client.add_domain_entry("http://test4.com")
client.add_domain_entry("http://test5.com")
client.add_domain_entry("http://test6.com")
client.add_domain_entry("http://test7.com")
"""

print(f"[CALL] {client.get_next_unassigned_domain_entry()}")

'''
for url in urls:
    print(scrape_webpage(url))

# Example usage
query = "example"
results = search_tfidf(query)
print("Search results using TF-IDF:", results)
'''
