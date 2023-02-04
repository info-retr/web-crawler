import logging
import re
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import tldextract
import string

import uu
import json
import heapq
from collections import defaultdict

from PartA import tokenize, tokenize_file

logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus
    # subdomains that it visited, how many different URLs it has
# processed from each of those subdomains.
        self.url_count_per_subdomain = defaultdict(int)
        self.valid_outlink_page_count: dict = {}
        self.valid_urls: set = set()
        self.trap_urls: set = set()
        # self.valid_urls: list = []
        # self.trap_urls: list = []
        self.page_word_counts: dict = {}
        self.shared_word_count: dict = {}
        # a single url_data dict of the previous link visited
        self.url_data_buffer: dict = {}

        self.stop_words = tokenize_file('stop_words.txt')
        self.stop_words.extend(list(string.ascii_letters))
        self.stop_words = sorted(set(self.stop_words))

    def get_subdomain(self, url):
        sub = tldextract.extract(url)
        if sub.subdomain in self.url_count_per_subdomain:
            self.url_count_per_subdomain[sub.subdomain] += 1
        else:
            self.url_count_per_subdomain[sub.subdomain] = 1

    def write_analytics(self):
        print("all this function should do is output data that already exists. the 2 methods take care of "
              "on their own with start_crawling as a sort of glue/backbone")
        
        with open('valid_urls.txt', 'w') as valid_file:
            for c in self.valid_urls:
                valid_file.write(c)

        with open('trapped_urls.txt', 'w') as trap_file:
            for t in self.trap_urls:
                trap_file.write(t)

        analytics_file = open('analytics.txt', 'w')
        analytics_file.write("1: subdomains visited and number of urls processed")
        analytics_file.write(json.dumps(self.url_count_per_subdomain))

        analytics_file.write("2: page with most valid outlinks")
        analytics_file.write(max(self.valid_outlink_page_count, key=self.valid_outlink_page_count.get))

        analytics_file.write("3: see trapped URLs in attached trapped_urls.txt, \
            and see valid URLs in attached valid_urls.txt ")

        analytics_file.write("4: longest page in terms of number of words")
        analytics_file.write(max(self.page_word_counts, key=self.page_word_counts.get))

        analytics_file.write("5: top 50 most common words across all pages")
        # file.write(json.dumps(heapq.nlargest(50, iterable)))
        analytics_file.close()
        print('analytics written')

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            # logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)

        self.valid_urls = sorted(self.valid_urls)
        self.trap_urls = sorted(self.trap_urls)
        with open('valid_urls.txt', 'w') as valid_file:
            for c in self.valid_urls:
                valid_file.write("{}\n".format(c))
        with open('trapped_urls.txt', 'w') as trap_file:
            for t in self.trap_urls:
                trap_file.write("{}\n".format(t))

    def extract_next_links(self, url_data) -> set:
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        outputLinks = set()
        if not ((url_data['content'] is None) or (url_data['size'] == 0) or (
                url_data['http_code'] in range(400, 600)) or (url_data['content_type'] == 'application/pdf') or (
                        url_data['content_type'] == 'application/zip')):
            soup = BeautifulSoup(url_data['content'], "lxml")
            for link in soup.findAll('a'):
                if url_data['is_redirected']:
                    outputLink = urljoin(url_data['final_url'], link.get('href'))
                else:
                    outputLink = urljoin(url_data['url'], link.get('href'))
                    if self.is_valid(outputLink):
                        self.valid_urls.add(outputLink)

                    else:
                        self.trap_urls.add(outputLink)
                        
                self.get_subdomain(outputLink)
                outputLinks.add(outputLink)
        # analyti-cizing 1,2,4,5
        # if self.url_data_buffer == {}:
        #     self.url_data_buffer = url_data
        # else:
        #     # 1
        #     # self.url_count_per_subdomain[tldextract.extract(outputLink).subdomain] += 1
        #     # 2
        #     counter = 0
        #     for outlink in outputLinks:
        #         if self.is_valid(outlink):
        #             counter += 1
        #     # self.valid_outlink_page_count[outputLink] = counter
        #     # 4
        #     self.append_page_word_count(url_data)
        #     # 5
        #     # self.append_shared_word_count(url_data)
        #
        #     self.url_data_buffer = url_data.copy()
        return outputLinks

    # def url_content_to_page_text(self, url_data) -> str:
    #     # page_contents = binascii.b2a_uu(url_data['content'])
    #     # page_contents = url_data['content'].decode("utf-8")
    #     # page_contents = str(url_data['content'], 'utf-8')
    #     # >> > str(my_byte_str, 'utf-8')
    #     page_contents = url_data['content']
    #     print(page_contents)
    #     return BeautifulSoup(page_contents, 'html.parser').get_text()

    def append_page_word_count(self, url_data):
        # print(BeautifulSoup(url_data['content'], 'html.parser').get_text())
        self.page_word_counts[url_data['url']] = len(tokenize(BeautifulSoup(url_data['content'], 'html.parser').get_text()))

# a word in any 1 file has to exist in all pages 
# dict {word:str, frequency}
# but we need another dstruct
# first, find a set intersection between page1 and page2
# strip away set elements that are also in stopwords
# shared words have to exist in both of them
# then find set intersection between set(p1,p2) and page3
#     def append_shared_word_count(url_data):
#         first_word_set: set = set(tokenize(url_content_to_page_text(url_data)))
#         second_word_set: set = set(tokenize(url_content_to_page_text(url_data)))
#         common = len(second_word_set.intersection(first_word_set))
#         self.shared_word_count[url_data['url']] =
#                     # res = any(ele in test_string for ele in test_list)
#             # stop words
#             if any(word in )
#         self.url_data_buffer
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
            # urls that are too long
            if ( len(url) > 120 ):
                return False

            # repeating patterns
            pattern = re.compile(r"(.)(/{2,})(.*)") #repeating patterns
            match = pattern.search(parsed.path)
            if match:
                return False

            # various directory and query arguments filtered for multiple reasons, such as:
            # pound sign indicating elements/positions all on the same page
            if ( ('#' in url) or ('/pix/' in url) or ('/cite/' in url) or ('/cites/' in url) or ('/rules/' in url) ):
                return False

            # dynamic url's that have quite a few query arguments, =login not being accessible thru crawling, action=implying absent user interactivity, etc
            if '?' in url or '=' in url or '&' in url:
                if ( ('=login' in url) or ('precision=second' in url) or ('=diff' in url) or ('version=' in url) or ('action=' in url) ): #or ('do=' in url)):
                    return False
                query_args: dict = parse_qs(urlparse(url).query)
                return len(query_args) < 6

            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf" \
                                    + "|bam|lif|ply|mexw|bw)$", parsed.path.lower())
        except TypeError:
            # print("TypeError for ", parsed)
            return False

# ================
# sources:
# https://edstem.org/us/courses/33063/discussion/2500737
# https://youtu.be/klJZw2aMEIQ?t=605
# https://docs.python.org/3/library/urllib.parse.html
# https://stackoverflow.com/questions/70717072/how-to-compare-urls-in-python-not-traditional-way
# https://stackoverflow.com/questions/24396406/find-most-common-words-from-a-website-in-python-3
# https://stackoverflow.com/questions/6925825/get-subdomain-from-url-using-python
# https://stackoverflow.com/questions/10113090/best-way-to-parse-a-url-query-string
# https://developers.google.com/search/blog/2008/09/dynamic-urls-vs-static-urls
# https://datagy.io/python-list-alphabet/
# https://www.w3schools.com/python/gloss_python_join_lists.asp
# https://www.ranks.nl/stopwords
# https://fleiner.com/bots/
# https://www.crummy.com/software/BeautifulSoup/bs4/doc/
# https://developer.mozilla.org/en-US/docs/Web/HTTP/Status#redirection_messages
# https://www.geeksforgeeks.org/http-headers-content-type/
# https://pypi.org/project/tldextract/
# https://docs.python.org/3/library/json.html
# https://stackoverflow.com/questions/268272/getting-key-with-maximum-value-in-dictionary
# https://pythontic.com/algorithms/heapq/nlargest
# https://stackoverflow.com/questions/1692388/python-list-of-dict-if-exists-increment-a-dict-value-if-not-append-a-new-dic
# https://www.w3schools.com/python/ref_set_update.asp
# https://www.programiz.com/python-programming/methods/dictionary/copy
