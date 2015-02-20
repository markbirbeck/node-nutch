/**
 * Class to handle crawl state:
 */

var CrawlState = function (now){
  this.state = CrawlState.UNFETCHED;
  this.fetchTime = now;
};

CrawlState.UNFETCHED = 'unfetched';
CrawlState.GENERATED = 'generated';
CrawlState.FETCHED = 'fetched';
CrawlState.GONE = 'gone';

module.exports = CrawlState;