'use strict';
const h = require('highland');

var request = require('request');
var path = require('path');

var gutil = require('gulp-util');

var CrawlState = require('../models/crawlState');
var config = require('../config/config');

/**
 * fetch: Fetch data using a list of URLs:
 *
 * See:
 *
 *  https://wiki.apache.org/nutch/Nutch2Crawling#Fetch
 */

var fetch = (crawlBase, cb) => {
  var taskName = 'fetch';
  var now = Date.now();

  return h(crawlBase.src())

    /**
     * Only process data sources that are ready to be fetched:
     */

    .filter(statusFile => {
      return statusFile.data.crawlState.state === CrawlState.GENERATED;
    })

    /**
     * For each item that is of the right status, fetch it:
     */

    .consume(function (err, statusFile, push, next){

      /*
       * Forward any errors:
       */

      if (err) {
        push(err);
        next();
        return;
      }

      /**
       * Check to see if we're finished:
       */

      if (statusFile === h.nil) {
        push(null, h.nil);
        return;
      }

      request(statusFile.data.url, function (err, response, body) {
        var headers;
        var status;

        if (err){
          status = err;
          headers = '';
          body = '';
        } else {
          status = response.statusCode;
          headers = response.headers;
        }
        var fetchedContent = new gutil.File({
          base: config.dir.CrawlBase,
          path: config.dir.CrawlBase + path.sep +
            encodeURIComponent(statusFile.data.url) + '/fetchedContent/content',
          contents: new Buffer(body)
        });

        crawlBase.filesDest(config.dir.CrawlBase)
          .write(fetchedContent);

        fetchedContent = new gutil.File({
          base: config.dir.CrawlBase,
          path: config.dir.CrawlBase + path.sep +
            encodeURIComponent(statusFile.data.url) + '/fetchedContent/headers',
          contents: new Buffer(JSON.stringify(headers))
        });
        crawlBase.filesDest(config.dir.CrawlBase)
          .write(fetchedContent);

        statusFile.data.fetchStatus = status;
        statusFile.data.crawlState.retries++;
        statusFile.data.crawlState.fetchTime = now;

        /**
         * Update the parse status:
         */

        delete statusFile.data.parseStatus;

        push(null, statusFile);
        next();
      });
    })
    .doto(statusFile => {
      if (statusFile.data.fetchStatus === 200) {
        console.info(
          '[%s] fetched \'%s\' (status=%d, retries=%d)',
          taskName,
          statusFile.data.url,
          statusFile.data.fetchStatus,
          statusFile.data.crawlState.retries
        );
      } else {
        console.error(
          '[%s] failed to fetch \'%s\' (status=%d, retries=%d)',
          taskName,
          statusFile.data.url,
          statusFile.data.fetchStatus,
          statusFile.data.crawlState.retries
        );
      }
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

module.exports = fetch;
