import logging
import re
from urllib.parse import urlparse
from lxml import html as lh
from bs4 import BeautifulSoup
from urllib.parse import urljoin

import string

# https://edstem.org/us/courses/33063/discussion/2500737
from PartA import tokenize, compute_word_frequencies, tokenize_file

logger = logging.getLogger(__name__)

URL_LEN_LIMIT = 200

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus
        self.visited_subdomains: dict = {}
        self.most_valid_out_links: dict = {}
        self.downloaded_urls: dict = {}
        self.longest_worded_page: dict = {}
        self.top_fifty_frequency_words: dict = {}
        self.stop_words = tokenize_file('stop_words.txt')
        self.stop_words.extend(list(string.ascii_lowercase))
        self.stop_words = sorted(set(self.stop_words))
        # print(self.stop_words)

    def write_analytics(self):
        file = open('analytics.txt', 'w')
        file.write("analytics")
        file.close()
        print('analytics written')

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        rabbit_hole_count = 0
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            # logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)
            
        self.write_analytics()

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        outputLinks = []
        # do i need all of these checks or just 404
        if not (url_data['content'] is None or url_data['size'] == 0 or url_data['http_code'] == 404):
            soup = BeautifulSoup(url_data['content'], "lxml")
            for link in soup.findAll('a'):
                outputLink = urljoin(url_data['url'], link.get('href'))
                outputLinks.append(outputLink)
        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        try:
            # ============start trap detection===========
            if len(url) > URL_LEN_LIMIT or '#' in url: 
                return False
            if re.match(".*\.(jpg|jpeg|png|gif|bmp|svg|pdf|mp4|mp3|ogg|webm|mov|avi)$", parsed.path.lower()): #non-textual data types
                return False
            pattern = re.compile(r"(.*)(/{2,})(.*)") #repeating patterns
            match = pattern.search(parsed.path)
            if match:
                return False

            # ============end trap detection=============
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

        except TypeError:
            # print("TypeError for ", parsed)
            return False

# sources:
# https://datagy.io/python-list-alphabet/
# https://www.w3schools.com/python/gloss_python_join_lists.asp
# https://www.ranks.nl/stopwords
# https://fleiner.com/bots/
# https://www.cis.uni-muenchen.de/~yeong/Kurse/ss09/WebDataMining/kap8_rev.pdf
# https://docs.python.org/3/library/re.html

