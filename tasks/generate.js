'use strict';
const h = require('highland');

var CrawlState = require('../models/crawlState');

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

var generate = (crawlBase, cb) => {
  var taskName = 'generate';
  var now = Date.now();

  return h(crawlBase.src())

    /**
     * Only process data sources that haven't been generated yet:
     */

    .filter(statusFile => {
      return statusFile.data.crawlState.state !== CrawlState.GENERATED;
    })

    /**
     * Also ensure that we only pick up URLs that it's time to fetch:
     */

    .filter(statusFile => {
      return statusFile.data.crawlState.fetchTime < now;
    })

    /**
     * Update the status to indicate that the URL is about to be fetched:
     */

    .doto(statusFile => {
      statusFile.data.crawlState.state = CrawlState.GENERATED;
      statusFile.data.crawlState.retries = 0;

      /**
       * Prevent this URL from being generated for another week:
       */

      statusFile.data.crawlState.fetchTime = now + (7 * 24 * 60 * 60 * 1000);
    })
    .doto(statusFile => {
      console.info('[%s] generated \'%s\'', taskName, statusFile.data.url);
    })

    /**
     * Update the crawl database with any changes:
     */

    .through(crawlBase.dest())

    /**
     * Let Gulp know that we're done:
     */

    .done(cb)
    ;
};

module.exports = generate;
