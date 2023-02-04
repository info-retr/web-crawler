import logging
import re
from urllib.parse import urlparse, parse_qs
from lxml import html as lh
from bs4 import BeautifulSoup
from urllib.parse import urljoin

import string

# https://edstem.org/us/courses/33063/discussion/2500737
from PartA import tokenize, compute_word_frequencies, tokenize_file

logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus
        self.init_analytics_data()
        self.init_stop_words()
        self.init_debug()

    def init_analytics_data(self):
        # 
        self.visited_subdomains: dict = {}
        self.page_with_the_most_valid_outlinks: dict = {}
        self.downloaded_urls: list = []
        self.trap_urls: list = []
        self.longest_worded_page: dict = {}
        self.top_fifty_frequency_words: dict = {}

    def init_stop_words(self):
        self.stop_words = tokenize_file('stop_words.txt')
        self.stop_words.extend(list(string.ascii_letters))
        self.stop_words = sorted(set(self.stop_words))
        self.url_count = 0
        # print(self.stop_words)

    def init_debug(self):
        self.linkList = []


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
        file = open('crawled_urls.txt', 'w')
        rabbit_hole_count = 0
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            # logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)
            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)
                        if next_link not in self.linkList:
                            self.linkList.append(next_link)
                            self.url_count += 1
                            file.write("{}\n".format(next_link))
        file.close()        
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
        # outputLinks = []
        # if not ( (url_data['content'] is None) or (url_data['size'] == 0) or (url_data['http_code'] in range(400,600)) or (url_data['content_type'] == 'application/pdf') or (url_data['content_type'] == 'application/zip') ):
        #     # print(url_data['url'], ':', url_data['http_code'])
        #     soup = BeautifulSoup(url_data['content'], "lxml")
        #     for link in soup.findAll('a'):
        #         outputLink = urljoin(url_data['url'], link.get('href'))
        #         outputLinks.append(outputLink)
        # return outputLinks
        outputLinks = []
        if not ( (url_data['content'] is None) or (url_data['size'] == 0) or (url_data['http_code'] in range(400,600)) or (url_data['content_type'] == 'application/pdf') or (url_data['content_type'] == 'application/zip') ):
            soup = BeautifulSoup(url_data['content'], "lxml")
            for link in soup.findAll('a'):
                if (url_data['is_redirected']):
                    outputLink = urljoin(url_data['final_url'], link.get('href'))
                else:
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
            
            # simplest checks
            if ((len(url) > 100) or ('#' in url) or ('/pix/' in url) or ('/cite/' in url) or ('/cites/' in url) or ('/rules/' in url) ): 
                # record trap in 
                return False

            # repeating patterns
            pattern = re.compile(r"(.)(/{2,})(.*)") #repeating patterns
            match = pattern.search(parsed.path)
            if match:
                return False

            # # 
            # if '?' in url or '=' in url or '&' in url:
            #     # url_parse = urlparse(url)
            #     query_args: dict = parse_qs(urlparse(url).query)
            #     return False
            if url.endswith('action=login') or url.endswith('precision=second'):
                return False

            # login pages
            if url.endswith('do=login&sectok=') or url.endswith('login') or url.endswith('login.php')  or url.endswith('login=1'):
                return False

            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf" \
                                    + "|bam|lif|ply|mexw)$", parsed.path.lower())
            # ============end trap detection=============

        except TypeError:
            # print("TypeError for ", parsed)
            return False


