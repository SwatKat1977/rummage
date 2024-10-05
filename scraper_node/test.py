import requests
from bs4 import BeautifulSoup
from collections import defaultdict
import math

# Dictionary to store indexed content: URL -> text content
web_index = defaultdict(str)

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

for url in urls:
    print(scrape_webpage(url))

# Example usage
query = "example"
results = search_tfidf(query)
print("Search results using TF-IDF:", results)
