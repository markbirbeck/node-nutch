var es = require('event-stream');

var filter = require('gulp-filter');

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

var generate = function (crawlBase){
  var now = Date.now();

  return crawlBase.src()

    /**
     * Only process data sources that haven't been generated yet:
     */

    .pipe(filter(function (file){
      return file.data.crawlState.state !== CrawlState.GENERATED;
    }))

    /**
     * Also ensure that we only pick up URLs that it's time to fetch:
     */

    .pipe(filter(function (file){
      return file.data.crawlState.fetchTime < now;
    }))

    /**
     * Update the status to indicate that the URL is about to be fetched:
     */

    .pipe(es.map(function (file, cb){
      file.data.crawlState.state = CrawlState.GENERATED;
      file.data.crawlState.retries = 0;

      /**
       * Prevent this URL from being generated for another week:
       */

      file.data.crawlState.fetchTime = now + (7 * 24 * 60 * 60 * 1000);
      cb(null, file);
    }))

    /**
     * Update the crawl database with any changes:
     */

    .pipe(crawlBase.dest());
};

module.exports = generate;
