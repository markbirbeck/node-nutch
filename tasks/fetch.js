var request = require('request');

var es = require('event-stream');
var through2 = require('through2');

var filter = require('gulp-filter');

var CrawlState = require('../models/CrawlState');
var FetchedContent = require('../models/FetchedContent');

/**
 * fetch: Fetch data using a list of URLs:
 *
 * See:
 *
 *  https://wiki.apache.org/nutch/Nutch2Crawling#Fetch
 */

var fetch = function (crawlBase){
  var taskName = 'fetch';
  var now = Date.now();

  return crawlBase.src()

    /**
     * Only process data sources that are ready to be fetched:
     */

    .pipe(filter(function (file){
      return file.data.crawlState.state === CrawlState.GENERATED;
    }))

    /**
     * Retrieve the document from the URL:
     */

    .pipe(es.map(function (file, cb){
      request(file.data.url, function (err, response, body) {
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

        file.data.fetchedContent = new FetchedContent(status, headers, body);
        file.data.crawlState.retries++;
        file.data.crawlState.fetchTime = now;
        cb(null, file);
      });
    }))

    /**
     * Update the crawl database with any changes:
     */

    .pipe(through2.obj(function (file, enc, cb){
      if (file.data.fetchedContent.status === 200) {
        console.info(
          '[%s] fetched \'%s\' (status=%d, retries=%d)',
          taskName,
          file.data.url,
          file.data.fetchedContent.status,
          file.data.crawlState.retries
        );
      } else {
        console.error(
          '[%s] failed to fetch \'%s\' (status=%d, retries=%d)',
          taskName,
          file.data.url,
          file.data.fetchedContent.status,
          file.data.crawlState.retries
        );
      }
      cb(null, file);
    }))
    .pipe(crawlBase.dest());
};

module.exports = fetch;
