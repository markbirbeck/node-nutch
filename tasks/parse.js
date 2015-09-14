var path = require('path');

var es = require('event-stream');

var filter = require('gulp-filter');

var ParseState = require('../models/parseState');

var Tika = require('tika');
var tika = new Tika();
var config = require('../config/config');

/**
 * parse: Parse content from fetched pages:
 *
 * See:
 *
 *  https://wiki.apache.org/nutch/Nutch2Crawling#Parse
 */

var parse = function (crawlBase){
  var taskName = 'parse';

  return crawlBase.src()

    /**
     * Only process data sources that have been fetched, and not parsed:
     */

    .pipe(filter(function (file){
      return (file.data.fetchedStatus === 200) &&
        (!file.data.parseStatus ||
          file.data.parseStatus.state === ParseState.NOTPARSED);
    }))
    .pipe(es.map(function (file, doneParsing){
      crawlBase.filesSrc(file.data.url, 'fetchedContent/content')
        .pipe(tika)
        .pipe(es.map(function(fetchedContent, cb) {
          fetchedContent.base = config.dir.CrawlBase + path.sep;
          fetchedContent.path = config.dir.CrawlBase + path.sep +
            encodeURIComponent(file.data.url) + path.sep + 'parse';
          file.data.parseStatus = new ParseState(ParseState.SUCCESS);
          cb(null, fetchedContent);
        }))
        .pipe(crawlBase.filesDest())
        .on('end', function() {
          doneParsing(null, file);
        });
    }))
    .pipe(es.map(function (file, cb){
      console.info(
        '[%s] parsed \'%s\' (parse state=%s)',
        taskName,
        file.data.url,
        file.data.parseStatus.state
      );
      cb(null, file);
    }))

    /**
     * Update the crawl database with any changes:
     */

    .pipe(crawlBase.dest());
};

module.exports = parse;
