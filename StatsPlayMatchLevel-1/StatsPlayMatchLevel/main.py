"""run the scraping process"""

# packages
from scrapy.utils.project import get_project_settings
from StatsPlayMatchLevel.spiders import nba_scraper, nfl_scraper, mlb_scraper, nhl_scraper
from time import sleep
import scrapy.crawler as crawler
from twisted.internet import reactor
from multiprocessing import Process, Queue
from scrapy.utils.log import configure_logging
import random

# pause time
pause_time_s: int = random.randint(3000, 3600)

# execute command pattern of crawlers
crawlers_commands: list = [mlb_scraper.MatchMLBScraper, nba_scraper.MatchNBAScraper, nfl_scraper.MatchNFLScraper,
                           nhl_scraper.MatchNHLScraper]

def f(q, spider):
    try:
        runner = crawler.CrawlerRunner(get_project_settings())
        deferred = runner.crawl(spider)
        deferred.addBoth(lambda _: reactor.stop())
        reactor.run()
        q.put(None)
    except Exception as e:
        q.put(e)

    return q

def run_spider(spider):
    q = Queue()
    p = Process(target=f, args=(q, spider))
    p.start()
    result = q.get()
    p.join()

    if result is not None:
        raise result

configure_logging()

if __name__ == "__main__":
    while True:
        for command in crawlers_commands:
            run_spider(command)
        sleep(pause_time_s)


# schedule job to run every period
#schedule.every(period_h).hours.do(job)

# run the scheduler in a loop
#while True:
    #job()
    #schedule.run_pending()
    #sleep(1800)
