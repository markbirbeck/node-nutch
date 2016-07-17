'use strict';
const h = require('highland');

var path = require('path');

var ParseState = require('../models/parseState');

// var Tika = require('tika');
var config = require('../config/config');

/**
 * parse: Parse content from fetched pages:
 *
 * See:
 *
 *  https://wiki.apache.org/nutch/Nutch2Crawling#Parse
 */

var parse = function (crawlBase, customParser, cb){
  var taskName = 'parse';

  return h(crawlBase.src())

    /**
     * Only process data sources that have been fetched, and not parsed:
     */

    .filter(statusFile => {
      return (statusFile.data.fetchStatus === 200) &&
        (!statusFile.data.parseStatus ||
          statusFile.data.parseStatus.state === ParseState.NOTPARSED);
    })

    /**
     * For each item that is of the right status parse the content:
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
       * Read the latest content for each URL in the crawl DB:
       */

      var url = statusFile.data.url;

      h(crawlBase.filesSrc(url, 'fetchedContent/content'))

        /**
         * Get a JSON version of the content:
         */

        .doto(fetchedContentFile => {
          fetchedContentFile.data = JSON.parse(String(fetchedContentFile.contents));
        })

        /**
         * Parse the fetched content:
         *
         * [TODO] At the moment we don't need to 'parse' because we're
         * reading JSON.
         */

        // .through(new Tika())

        /**
         * Don't bother processing if the parsed content has not changed:
         */

        .filter(parseContentFile => {
          return parseContentFile.contents.toString() !== statusFile.data.parseContent;
        })

        /**
         * Set up the path for saving the parsed data to:
         */

        .doto(parseContentFile => {

          /**
           * [TODO] Shouldn't keep reading from config; instead, get
           * settings from the crawlBase object.
           */

          parseContentFile.base = config.dir.CrawlBase + path.sep;
          parseContentFile.path = config.dir.CrawlBase + path.sep +
            encodeURIComponent(url) + path.sep + 'parse';
        })

        /**
         * Write the parsed files:
         */

        .through(crawlBase.filesDest())

        /**
         * Update the parse status:
         */

        .doto(parseContentFile => {
          statusFile.data.parseContent = parseContentFile.contents.toString();
          statusFile.data.parseStatus = new ParseState(ParseState.SUCCESS);
        })

        /**
         * Update the extract status:
         */

        .doto(() => {
          delete statusFile.data.extractStatus;
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
      let state = (statusFile.data.parseStatus) ? statusFile.data.parseStatus.state :
        'state not set!!!';

      console.info(`[${taskName}] parsed '${statusFile.data.url}' (parse state=${state})`);
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

module.exports = parse;
