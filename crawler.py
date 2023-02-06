import logging
import re
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import tldextract
import string
from collections import defaultdict

from PartA import tokenize, tokenize_file, compute_word_frequencies

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
        self.valid_outlink_page_count = defaultdict(int)
        self.valid_urls: list = list()
        self.trap_urls: set = set()
        self.page_word_counts = defaultdict(int)
        # a single url_data dict of the previous link visited
        self.url_data_buffer: dict = {}

        self.stop_words = tokenize_file('stop_words.txt')
        self.stop_words.extend(list(string.ascii_letters))
        self.stop_words = sorted(set(self.stop_words))
        self.top_fifty_frequency_words = defaultdict(int)

        self.common_web_file_exts: list = [
            "html", "htm", "css",'rss',"js","jsx","less","scss","wasm",
            "php",'shtml','xhtml','asp','asx'
          ]

    def get_subdomain(self, url):  # 1
        sub = tldextract.extract(url)
        if sub.subdomain in self.url_count_per_subdomain:
            self.url_count_per_subdomain[sub.subdomain] += 1
        else:
            self.url_count_per_subdomain[sub.subdomain] = 1

    def mostCommonWords(self, soup):  # 5
        words = tokenize(soup.get_text())
        for word in words:
            if str(word).isalpha():
                if str(word).lower() not in self.stop_words:
                    self.top_fifty_frequency_words[word] += 1

    def findLongestPage(self, url, soup):  # 4
        num_words = len(tokenize(soup.get_text()))
        self.page_word_counts[url] += num_words

    def write_analytics(self):
        try:
            print("writing analytics...")
            self.valid_urls = sorted(self.valid_urls)
            self.trap_urls = sorted(self.trap_urls)
            with open('valid_urls.txt', 'w') as valid_file:
                for c in self.valid_urls:
                    valid_file.write("{}\n".format(c))
            with open('trapped_urls.txt', 'w') as trap_file:
                for t in self.trap_urls:
                    trap_file.write("{}\n".format(t))            

            analytics_file = open('analytics.txt', 'w')
            analytics_file.write("(1) number of urls processed for all visited subdomains:\n\n")
            for k, v in self.url_count_per_subdomain.items():
                analytics_file.write("{}: {}\n".format(str(k), str(v)))

            analytics_file.write("\n(2) page with most valid outlinks:\n\n")
            max_links = max(self.valid_outlink_page_count, key=self.valid_outlink_page_count.get)
            analytics_file.write(
                "{} has {} valid outlinks\n".format(str(max_links), str(self.valid_outlink_page_count[max_links])))

            analytics_file.write("\n(3) list of downloaded URLs and identified traps:\n\n")
            analytics_file.write("see trapped URLs in trapped_urls.txt and valid URLs in valid_urls.txt\n")

            analytics_file.write("\n(4) page with highest word count:\n\n")
            longest_page = max(self.page_word_counts, key=self.page_word_counts.get)
            analytics_file.write(
                "{} has {} words\n".format(str(longest_page), str(self.page_word_counts[longest_page])))

            analytics_file.write("\n(5) top 50 common words across all pages\n\n")
            sorted_top50 = sorted(self.top_fifty_frequency_words.items(), key=lambda x: x[1], reverse=True)[:50]
            for i, s in enumerate(sorted_top50):
                analytics_file.write('{}: {}\n'.format(str(i + 1), str(s)))

            analytics_file.close()
            print('\nanalytics written')
        except ValueError:
            print('possibly empty corpus')

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

    def extract_next_links(self, url_data) -> list:
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        outputLinks = list()
        if not ((url_data['content'] is None) or (url_data['size'] == 0) or (url_data['http_code'] in range(400, 600))
                or 'text/html' not in str(url_data['content_type']) or 'index of' in str(url_data['content'].lower()) ):
            #or 'iso' in str(url_data['content_type']).lower()):
            # or (not url_data['is_redirected'] and url == url_data['final_url']) ):
            url = url_data['url']
            soup = BeautifulSoup(url_data['content'], "lxml")
            for link in soup.findAll('a'):
                if url_data['is_redirected']:
                    outputLink = urljoin(url_data['final_url'], link.get('href'))
                else:
                    outputLink = urljoin(url_data['url'], link.get('href'))
                if self.is_valid(outputLink):
                    outputLinks.append(outputLink)
                    self.valid_outlink_page_count[url] += 1
            if self.is_valid(url):
                self.valid_urls.append(url)
                self.findLongestPage(url, soup=soup)  # 4
                self.mostCommonWords(soup=soup)  # 5
                self.get_subdomain(url)
        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            self.trap_urls.add(url)
            return False
        try:
            if len(url) > 100:
                self.trap_urls.add(url)
                return False

            # various directory and query arguments filtered for multiple reasons, such as:
            # pound sign indicating elements/positions all on the same page
            if '#' in url:
                self.trap_urls.add(url)
                return False

            if '/pix/' in url:  # or ('/cite/' in url) or ('/cites/' in url) or ('/rules/' in url) ):
                self.trap_urls.add(url)
                return False

            if str(parsed.hostname).split('.')[-3:] != ['ics', 'uci', 'edu']:
                self.trap_urls.add(url)
                return False

            if ('=login' in url) or ('action=' in url):  # or ('do=' in url)):
                self.trap_urls.add(url)
                return False

            # repeating patterns
            pattern = re.compile(r"(.)(/{2,})(.*)")  # repeating patterns
            match = pattern.search(parsed.path)
            if match:
                self.trap_urls.add(url)
                return False

            # if (re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
            #                         + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            #                         + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            #                         + "|thmx|mso|arff|rtf|jar|csv" \
            #                         + "|rm|smil|wmv|swf|wma|zip|rar|gz" \
            #                         + "|bam|ply|mexw|bw|motif|pickle|mexmaci|svg|sh)$", parsed.path.lower())):
            # return False
            if (re.match('.*\.('+'|'.join(self.common_web_file_exts)+')$', parsed.path.lower())) or '.' not in parsed.path.lower():
                return True
            else:
                self.trap_urls.add(url)
                return False
        except TypeError:
            print("TypeError for ", parsed)
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
# https://www.youtube.com/watch?v=ng2o98k983k
# https://stackoverflow.com/questions/4776924/how-to-safely-get-the-file-extension-from-a-url
# https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type
# https://stackoverflow.com/questions/30997420/what-are-fragment-urls-and-why-to-use-them#:~:text=A%20fragment%20is%20an%20internal,name%20attribute%20matching%20the%20fragment.
# https://github.com/dyne/file-extension-list/blob/master/pub/categories.json
# https://www.freecodecamp.org/news/python-list-to-string-join-example/