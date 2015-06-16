var url = require('url');

var es = require('event-stream');
var through2 = require('through2');

var filter = require('gulp-filter');

var ParseState = require('../models/parseState');

var cheerio = require('cheerio');

/**
 * parse: Parse content from fetched pages:
 *
 * See:
 *
 *  https://wiki.apache.org/nutch/Nutch2Crawling#Parse
 */

var parse = function (crawlBase, customParser, customParseChanged){
  var taskName = 'parse';

  return crawlBase.src()

    /**
     * Only process data sources that have been fetched, and not parsed:
     */

    .pipe(filter(function (file){
      return (file.data.fetchedContent.status === 200) &&
        (!file.data.parseStatus ||
          file.data.parseStatus.state === ParseState.NOTPARSED);
    }))

    /**
     * Update the status to indicate that the URL is about to be fetched:
     */

    .pipe(es.map(function (file, cb){
      var $ = cheerio.load(file.data.fetchedContent.content);
      file.data.parse = {
        title: $('title').text().trim(),
        outlist: $('a').map(function (){
          var title;
          var _s;

          if ($(this).attr('title')){
            _s = '@title';
            title = $(this).attr('title');
          } else {
            title = $(this).text().trim();
            _s = 'text';
            if (title === ''){
              if ($('img', this)){
                _s = 'img[@alt]';
                title = $('img', this).attr('alt');
              }
            }
          }

          return {
            url: url.resolve(file.data.url, $(this).attr('href') || ''),
            _s: _s,
            title: title
          };
        }).get()
      };
      file.data.parseStatus = new ParseState(ParseState.SUCCESS);
      cb(null, file);
    }))

    /**
     * Call any custom parser:
     */

    .pipe(es.map(function (file, cb){
      if (customParser){
        var customParse = customParser(file.data.fetchedContent.content);

        if (customParse){

          /**
           * Use the provided function to check if the custom parse value has
           * changed:
           */

          if (customParseChanged){
            file.data.customParseChanged = customParseChanged(customParse, file.data.customParse);
          }

          /**
           * Make sure to only overwrite old value after it has been used to
           * check for changes:
           */

          file.data.customParse = customParse;
          file.data.customParseStatus = new ParseState(ParseState.SUCCESS);
        }
      }
      cb(null, file);
    }))

    /**
     * Update the crawl database with any changes:
     */

    .pipe(through2.obj(function (file, enc, cb){
      console.info(
        '[%s] parsed \'%s\' (parse state=%s, custom parse state=%s)',
        taskName,
        file.data.url,
        file.data.parseStatus.state,
        file.data.customParseStatus.state
      );
      cb(null, file);
    }))
    .pipe(crawlBase.dest());
};

module.exports = parse;
