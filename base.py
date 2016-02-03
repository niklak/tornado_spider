import time
import logging
import json
import csv
from datetime import timedelta
from collections import defaultdict
from urllib.parse import urljoin, urldefrag
from tornado import httpclient, gen, queues
from lxml import html


class BQueue(queues.Queue):
    """ Bureaucratic queue """

    def __init__(self, maxsize=0, capacity=0):
        """
        :param maxsize: a default maxsize from tornado.queues.Queue,
            means maximum queue members at the same time.
        :param capacity: means a quantity of income tries before will refuse
            accepting incoming data
        :return:
        """
        super().__init__(maxsize)
        if capacity is None:
            raise TypeError("capacity can't be None")

        if capacity < 0:
            raise ValueError("capacity can't be negative")

        self.capacity = capacity
        self.put_counter = 0
        self.is_reached = False

    def put_nowait(self, item):

        if not self.is_reached:
            super().put_nowait(item)
            self.put_counter += 1

            if 0 < self.capacity == self.put_counter:
                self.is_reached = True


class BaseWebSpider:
    OUTPUT_FORMATS = ['json', 'csv']

    def __init__(self, base_url, capture_pattern, concurrency=2, timeout=300,
                 delay=0, headers=None, exclude=None, verbose=True,
                 output='json', max_crawl=0, max_parse=0, start_url=None):

        assert output in self.OUTPUT_FORMATS, 'Unsupported output format'

        self.output = output

        self.base = base_url
        self.start_url = self.base if not start_url else start_url
        self.capture = capture_pattern
        self.exclude = exclude if isinstance(exclude, list) else []

        self.concurrency = concurrency
        self.timeout = timeout
        self.delay = delay

        self.q_crawl = BQueue(capacity=max_crawl)
        self.q_parse = BQueue(capacity=max_parse)

        self.brief = defaultdict(set)
        self.data = []

        self.log = logging.getLogger()
        self.log.setLevel(logging.INFO)

        if not verbose:
            self.log.disabled = True

        self.client = httpclient.AsyncHTTPClient(force_instance=True,
                                                 defaults=headers)

    def get_parsed_content(self, url):
        """
        :param url: an url from which html will be parsed.
        :return: it has to return a dict with data.
        Must be wrapped in a gen.coroutine.
        """
        raise NotImplementedError

    @gen.coroutine
    def get_urls(self, document):
        urls = []
        urls_to_parse = []
        dom = html.fromstring(document)
        for href in dom.xpath('//a/@href'):
            if any(e in href for e in self.exclude):
                    continue
            url = urljoin(self.base, urldefrag(href)[0])
            if url.startswith(self.base):
                if self.capture in url:
                    urls_to_parse.append(url)
                urls.append(url)
        return urls, urls_to_parse

    @gen.coroutine
    def get_html_from_url(self, url):

        try:
            response = yield self.client.fetch(url)
            doc = response.body if isinstance(response.body, str) \
                else response.body.decode()
        except:
            self.log.error('Error during fetching urls!', exc_info=True)
            raise gen.Return([])
        return doc

    @gen.coroutine
    def get_links_from_url(self, url):
        document = yield self.get_html_from_url(url)
        urls = yield self.get_urls(document)
        return urls

    @gen.coroutine
    def __wait(self, name):
        if self.delay > 0:
            self.log.info('{} waits for {} sec.'.format(name, self.delay))
            yield gen.sleep(self.delay)

    @gen.coroutine
    def crawl_url(self):
        current_url = yield self.q_crawl.get()
        try:
            if current_url in self.brief['crawling']:
                self.log.info('Skipping the duplicate')
                return    # go to finally block before return

            self.log.info('Crawling: {}'.format(current_url))
            self.brief['crawling'].add(current_url)
            urls, urls_to_parse = yield self.get_links_from_url(current_url)
            self.brief['crawled'].add(current_url)

            for url in urls:
                if self.q_crawl.is_reached:
                    self.log.warning('I do not have to crawl anymore.')
                    break
                yield self.q_crawl.put(url)

            for url in urls_to_parse:
                if self.q_parse.is_reached:
                    self.log.warning('I do not have to parse anymore')
                    break

                if url not in self.brief['parsing']:
                    yield self.q_parse.put(url)
                    self.brief['parsing'].add(url)
                    self.log.info('Captured: {}'.format(url))
        finally:
            self.q_crawl.task_done()

    @gen.coroutine
    def parse_url(self):
        url_to_parse = yield self.q_parse.get()
        self.log.info('Parsing: {}'.format(url_to_parse))
        try:
            content = yield self.get_parsed_content(url_to_parse)
            self.data.append(content)
        finally:
            self.q_parse.task_done()

    @gen.coroutine
    def crawler(self):
        while True:
            yield self.crawl_url()
            yield self.__wait('Crawler')

    @gen.coroutine
    def parser(self):
        while True:
            qsize = self.q_parse.qsize()
            if qsize > 0:
                self.log.info('PARSING QUEUE is about {} members'.format(qsize))
                yield self.parse_url()
            else:
                yield gen.sleep(0.5)

            yield self.__wait('Parser')
        return

    def _write_json(self, name):

        with open('{}-{}.json'.format(name, time.time()), 'w') as file:
            json.dump(self.data, file)

    def _write_csv(self, name):
        headers = self.data[0].keys()    # Make it better
        with open('{}-{}.csv'.format(name, time.time()), 'w') as csvfile:
            writer = csv.DictWriter(csvfile, headers)
            writer.writeheader()
            writer.writerows(self.data)

    @gen.coroutine
    def run(self):
        start = time.time()

        print('Start working')

        self.q_crawl.put(self.start_url)

        for _ in range(self.concurrency):
            self.crawler()
            self.parser()

        yield self.q_crawl.join(timeout=timedelta(seconds=self.timeout))
        yield self.q_parse.join()

        end = time.time()
        print('Parsing done in {} seconds'.format(end - start))

        assert self.brief['crawling'] == self.brief['crawled'], \
            'Crawling and crawled urls do not match'

        assert len(self.brief['parsing']) == len(self.data), \
            'PARSING != PARSED'

        self.log.info('Total crawled: {}'.format(len(self.brief['crawled'])))
        self.log.info('Total parsed: {}'.format(len(self.data)))
        self.log.info('Starting write to file')

        name = self.base.split('//')[1].replace('www', '').replace('/', '')
        if self.output == 'json':
            self._write_json(name)
        elif self.output == 'csv':
            self._write_csv(name)

        print('Parsed data has been stored.')
        print('Task done!')
