var request = require('request');

var es = require('event-stream');

var filter = require('gulp-filter');

var crawlBase = require('../models/crawlBase');
var CrawlState = require('../models/CrawlState');
var FetchedContent = require('../models/FetchedContent');

/**
 * fetch: Fetch data using a list of URLs:
 *
 * See:
 *
 *  https://wiki.apache.org/nutch/Nutch2Crawling#Fetch
 */

var fetch = function (){
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
        cb(null, file);
      });
    }))

    /**
     * Update the crawl database with any changes:
     */

    .pipe(crawlBase.dest());
};

module.exports = fetch;
