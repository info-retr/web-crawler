import atexit
import logging

import sys

from corpus import Corpus
from crawler import Crawler
from frontier import Frontier

if __name__ == "__main__":
    # Configures basic logging
    logging.basicConfig(format='%(asctime)s (%(name)s) %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.INFO)

    # Instantiates frontier and loads the last state if exists
    frontier = Frontier()
    frontier.load_frontier()

    # Instantiates corpus object with the given cmd arg
    corpus = Corpus(sys.argv[1])

    # Registers a shutdown hook to save frontier state upon unexpected shutdown
    # atexit.register(frontier.save_frontier)

    # Instantiates a crawler object and starts crawling
    crawler = Crawler(frontier, corpus)
    crawler.start_crawling()

    # file = open('url_count_per_subdomain.txt', 'w')
    # for k, v in crawler.url_count_per_subdomain.items():
    #     file.write("{}: {}\n".format(str(k), str(v)))
    # file.close()
    #
    # crawler.write_analytics()