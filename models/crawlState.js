/**
 * Class to handle crawl state:
 */

var CrawlState = function (state){
  this.state = state || CrawlState.UNFETCHED;
};

CrawlState.UNFETCHED = 'unfetched';
CrawlState.GENERATED = 'generated';
CrawlState.FETCHED = 'fetched';
CrawlState.GONE = 'gone';

module.exports = CrawlState;