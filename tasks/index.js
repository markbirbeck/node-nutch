'use strict';
const h = require('highland');

var elasticsearch = require('vinyl-elasticsearch');

var ExtractState = require('../models/extractState');
var config = require('../config/config');

var index = (crawlBase, cb) => {
  var taskName = 'index';

  return h(crawlBase.src())

    /**
     * Only process data sources that have been extracted:
     */

    .filter(statusFile => {
      return (statusFile.data.extractStatus &&
        (statusFile.data.extractStatus.state === ExtractState.SUCCESS));
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
       * Read the extracted content for each URL in the crawl DB:
       */

      var url = statusFile.data.url;

      h(crawlBase.filesSrc(url, 'extract'))

        /**
         * Get a JSON version of the content:
         */

        .doto(extractedContentFile => {
          extractedContentFile.data = JSON.parse(String(extractedContentFile.contents));
        })

        /**
         * If any fields use the params/val format then convert to the
         * direct format:
         */

        .doto(item => {
          let obj = item.data;

          if (obj) {
            Object.keys(obj).forEach(function(k) {
              var member = obj[k];

              if (member.val) {
                obj[k] = member.val;
                console.log('Changed: %s', k);
              }
            });
          }
        })

        /**
         * Store each line of input:
         */

        .through(elasticsearch.dest({
            index: 'calendar.place'
          },
          config.elastic
        ))

        /**
         * Update the indexed status:
         */

        .doto(indexedContentFile => {
          statusFile.data.indexedContent = indexedContentFile.contents.toString();
          // statusFile.data.indexStatus = new IndexState(IndexState.SUCCESS);
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
      console.info(`[${taskName}] indexed '${statusFile.data.url}'`);
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

module.exports = index;
