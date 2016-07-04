'use strict';
const h = require('highland');

var path = require('path');

var es = require('event-stream');
var through2 = require('through2');

var filter = require('gulp-filter');

var ParseState = require('../models/parseState');

var Tika = require('tika');
var config = require('../config/config');

/**
 * parse: Parse content from fetched pages:
 *
 * See:
 *
 *  https://wiki.apache.org/nutch/Nutch2Crawling#Parse
 */

var parse = function (crawlBase, customParser, customParseChanged, cb){
  var taskName = 'parse';

  return h(crawlBase.src())

    /**
     * Only process data sources that have been fetched, and not parsed:
     */

    .filter(file => {
      return (file.data.fetchedStatus === 200) &&
        (!file.data.parseStatus ||
          file.data.parseStatus.state === ParseState.NOTPARSED);
    })
    .flatMap(function(file) {
      console.log('In flatMap');
      return h(function(push/*, next*/) {
        let onDone = () => {
          push(null, file);
          push(null, h.nil);
        };

        h(crawlBase.filesSrc(file.data.url, 'fetchedContent/content'))
          .through(new Tika())
          .map(function(fetchedContent) {
            console.log('In parsing post Tika:', fetchedContent);
            fetchedContent.base = config.dir.CrawlBase + path.sep;
            fetchedContent.path = config.dir.CrawlBase + path.sep +
              encodeURIComponent(file.data.url) + path.sep + 'parse';
            file.data.parseStatus = new ParseState(ParseState.SUCCESS);
            return fetchedContent;
          })
          .doto(() => {
            console.log('In parsing, about to write to filesDest');
          })
          .through(h(crawlBase.filesDest()))
          // .resume()
          // .error(err => {
          //   console.error('Some kind of error!:', err);
          // })
          .done(function() {
            console.log('In end');
            onDone();
          })
          ;

      });
    })
    // .pipe(es.map(function (file, doneParsing){
    //   crawlBase.filesSrc(file.data.url, 'fetchedContent/content')
    //     .map(function(data) {
    //       var customParse = customParser(file.data);

    //       if (customParse) {
    //         file.data.customParse = customParse;
    //       }
    //       return file;
    //     })
    //     .pipe(es.map(function(fetchedContent, cb) {
    //       console.log('In parsing post custom parse:', fetchedContent);

          /**
           * Use the provided function to check if the custom parse value has
           * changed:
           */

        //   if (customParseChanged){
        //     fetchedContent.data.customParseChanged = customParseChanged(customParse,
        //       fetchedContent.data.customParse);
        //   }
        //   fetchedContent.base = config.dir.CrawlBase + path.sep;
        //   fetchedContent.path = config.dir.CrawlBase + path.sep +
        //     encodeURIComponent(file.data.url) + path.sep + 'customParse';
        //   file.data.customParseStatus = new ParseState(ParseState.SUCCESS);
        //   cb(null, fetchedContent);
        // }))
        // .pipe(es.map(function(file, cb) {
        //   console.log('In custom parsing, about to write to filesDest');
        //   cb(null, file);
        // }))
        // .pipe(crawlBase.filesDest())
        // // .resume()
        // .on('error', function(err) {
        //   console.error('Some kind of error!:', err);
        // })
        // .on('end', function() {
        //   console.log('In end of customer parse');
        //   doneParsing(null, file);
        // });
    // }))
    // .pipe(es.map(function (file, cb){
    //   if (customParser){
    //     var customParse = customParser(file.data.fetchedContent.content);

    //     if (customParse){

          /**
           * Make sure to only overwrite old value after it has been used to
           * check for changes:
           */

    //       file.data.customParse = customParse;
    //       file.data.customParseStatus = new ParseState(ParseState.SUCCESS);
    //     }
    //   }
    //   cb(null, file);
    // }))
    .doto(file => {
      let state = (file.data.parseStatus) ? file.data.parseStatus.state :
        'state not set!!!';

      console.info(`[${taskName}] parsed '${file.data.url}' (parse state=${state})`);
    })

    /**
     * Update the crawl database with any changes:
     */

    .through(crawlBase.dest())
    .done(cb);
};

module.exports = parse;
