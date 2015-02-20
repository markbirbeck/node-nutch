var es = require('event-stream');

var filter = require('gulp-filter');

var crawlBase = require('../models/crawlBase');
var CrawlState = require('../models/CrawlState');

/**
 * generate: Creates a new batch. Selects urls to fetch from the webtable
 * (or: marks urls in the webtable which need to be fetched).
 *
 * For now we're only supporting one batch:
 *
 * See:
 *
 *  https://wiki.apache.org/nutch/Nutch2Crawling#Generate
 */

var generate = function (){
  return crawlBase.src()

    /**
     * Only process data sources that haven't been fetched yet:
     */

    .pipe(filter(function (file){
      return file.data.crawlState.state === CrawlState.UNFETCHED;
    }))

    /**
     * Update the status to indicate that the URL is about to be fetched:
     */

    .pipe(es.map(function (file, cb){
      file.data.crawlState.state = CrawlState.GENERATED;
      file.data.crawlState.retries = 0;
      cb(null, file);
    }))

    /**
     * Update the crawl database with any changes:
     */

    .pipe(crawlBase.dest());
};

module.exports = generate;
