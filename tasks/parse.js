var url = require('url');

var es = require('event-stream');

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

var parse = function (crawlBase, customParser){
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
          file.data.customParse = customParse;
          file.data.customParseStatus = new ParseState(ParseState.SUCCESS);
        }
      }
      cb(null, file);
    }))

    /**
     * Update the crawl database with any changes:
     */

    .pipe(crawlBase.dest());
};

module.exports = parse;
