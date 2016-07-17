'use strict';
const h = require('highland');

var path = require('path');

var ExtractState = require('../models/extractState');
var ProcessState = require('../models/processState');

var config = require('../config/config');

/**
 * process: Process extracted content
 */

var parse = function (crawlBase, processFn, cb){
  var taskName = 'process';

  return h(crawlBase.src())

    /**
     * Only process data sources that have been extracted, and not processed:
     */

    .filter(file => {
      return (
        file.data.extractStatus &&
          (file.data.extractStatus.state === ExtractState.SUCCESS) &&
        (!file.data.processStatus ||
          (file.data.processStatus &&
          (file.data.processStatus.state !== ProcessState.SUCCESS))
        ) &&
        file.data.prevExtractContent && file.data.extractContent
      );
    })
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

      h(crawlBase.filesSrc(statusFile.data.url, 'extract'))

        /**
         * Get a JSON version of the extracted content:
         */

        .doto(extractContentFile => {
          extractContentFile.data = JSON.parse(String(extractContentFile.contents));
        })

        /**
         * Process the extracted content:
         */

        .doto(extractContentFile => {
          extractContentFile.data = processFn(statusFile.data);
        })

        /**
         * Convert the JSON back to a buffer:
         */

        .doto(extractContentFile => {
          extractContentFile.contents = new Buffer(JSON.stringify(extractContentFile.data));
        })

        /**
         * Set up the path for saving the processed data to:
         */

        .doto(processContentFile => {

          /**
           * [TODO] Shouldn't keep reading from config; instead, get
           * settings from the crawlBase object.
           */

          processContentFile.base = config.dir.CrawlBase + path.sep;
          processContentFile.path = config.dir.CrawlBase + path.sep +
            encodeURIComponent(statusFile.data.url) + path.sep + 'process';
        })
        .doto((processContentFile) => {
          console.log(`In processing, about to write to filesDest: ${processContentFile.path}`);
        })
        .through(crawlBase.filesDest())
        // .resume()
        // .error(err => {
        //   console.error('Some kind of error!:', err);
        // })
        .doto(processContentFile => {
          console.log('Updating status files');
          statusFile.data.processContent = processContentFile.contents.toString();
          statusFile.data.processStatus = new ProcessState(ProcessState.SUCCESS);
        })
        .done(function() {
          console.log('In process end');
          push(null, statusFile);
          next();
        })
        ;
    })
    .doto(statusFile => {
      let state = (statusFile.data.processStatus) ? statusFile.data.processStatus.state :
        'process state not set!!!';

      console.info(`[${taskName}] processed '${statusFile.data.url}' (process state=${state})`);
    })

    /**
     * Update the crawl database with any changes:
     */

    .through(crawlBase.dest())
    .done(cb);
};

module.exports = parse;
