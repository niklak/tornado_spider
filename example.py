from tornado import gen, ioloop
from bs4 import BeautifulSoup
from base import BaseWebSpider


class MyWebSpider(BaseWebSpider):

    @gen.coroutine
    def get_parsed_content(self, url):
        document = yield self.get_html_from_url(url)
        bs = BeautifulSoup(document, 'lxml')
        title = bs.find('title')
        if title:
            title = title.get_text().replace('- Wikipedia, the free encyclopedia', '')
        lastmod = bs.find('', {'id': 'footer-info-lastmod'})
        if lastmod:
            lastmod = lastmod.get_text().replace('This page was last modified on ', '')

        return {'title': title, 'lastmod': lastmod}

if __name__ == '__main__':

    base_url = 'https://en.wikipedia.org/'
    capture = '/wiki/'
    exclude = [':']
    concurrency = 2
    output = 'csv'
    max_crawl = 10
    max_parse = 10
    delay = 0
    headers = {"user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) "
                             "AppleWebKit 537.36 (KHTML, like Gecko) Chrome",
               "accept": "text/html,application/xhtml+xml,"
                         "application/xml;q=0.9,image/webp,*/*;q=0.8"}
    web_crawler = MyWebSpider(base_url, capture, concurrency, timeout=60,
                              headers=headers, exclude=exclude, output=output,
                              max_crawl=max_crawl, max_parse=max_parse)

    io_loop = ioloop.IOLoop.current()
    io_loop.run_sync(web_crawler.run)
