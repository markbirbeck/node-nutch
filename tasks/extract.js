'use strict';
const h = require('highland');

var path = require('path');

var ExtractState = require('../models/extractState');
var ParseState = require('../models/parseState');
var config = require('../config/config');

var extract = (crawlBase, extractFn, cb) => {
  var taskName = 'extract';

  return h(crawlBase.src())

    /**
     * Only process data sources that have been parsed but not extracted:
     */

    .filter(statusFile => {
      return (
        statusFile.data.parseStatus &&
          (statusFile.data.parseStatus.state === ParseState.SUCCESS) &&
        (!statusFile.data.extractStatus ||
          (statusFile.data.extractStatus &&
          (statusFile.data.extractStatus.state !== ExtractState.SUCCESS))
        )
      );
    })

    /**
     * For each item that is of the right status extract the content:
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

      /**
       * Read the latest parsed content for each URL in the crawl DB:
       */

      var url = statusFile.data.url;

      h(crawlBase.filesSrc(url, 'parse'))

        /**
         * Get a JSON version of the content:
         */

        .doto(parseContentFile => {
          parseContentFile.data = JSON.parse(String(parseContentFile.contents));
        })

        /**
         * Run the custom extractor:
         */

        .doto(extractContentFile => {
          extractContentFile.data = extractFn(extractContentFile.data);
        })

        /**
         * Don't bother processing if the extracted content has not changed:
         */

        .filter(extractContentFile => {
          return JSON.stringify(extractContentFile.data) !== statusFile.data.extractContent;
        })

        /**
         * Set up the path for saving the extracted data to:
         */

        .doto(extractContentFile => {

          /**
           * [TODO] Shouldn't keep reading from config; instead, get
           * settings from the crawlBase object.
           */

          extractContentFile.base = config.dir.CrawlBase + path.sep;
          extractContentFile.path = config.dir.CrawlBase + path.sep +
            encodeURIComponent(url) + path.sep + 'extract';
        })

        /**
         * Convert the JSON back to a buffer:
         */

        .doto(parseContentFile => {
          parseContentFile.contents = new Buffer(JSON.stringify(parseContentFile.data));
        })

        /**
         * Write the parsed files:
         */

        .through(crawlBase.filesDest())

        /**
         * Update the extracted status:
         */

        .doto(extractContentFile => {
          statusFile.data.prevExtractFetchTime = statusFile.data.extractFetchTime;
          statusFile.data.extractFetchTime = statusFile.data.crawlState.fetchTime;
          statusFile.data.prevExtractContent = statusFile.data.extractContent;
          statusFile.data.extractContent = extractContentFile.contents.toString();
          statusFile.data.extractStatus = new ExtractState(ExtractState.SUCCESS);
        })
        /**
         * Update the process status:
         */

        .doto(() => {
          delete statusFile.data.processStatus;
        })

        /**
         * Finally, indicate that we're finished this nested pipeline:
         */

        .done(function() {
          push(null, statusFile);
          next();
        })
        ;
    })
    .doto(statusFile => {
      console.info(`[${taskName}] extracted '${statusFile.data.url}'`);
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

module.exports = extract;
