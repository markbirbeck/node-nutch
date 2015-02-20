var path = require('path');
var url = require('url');
var fs = require('fs');

var es = require('event-stream');
var through2 = require('through2');
var runSequence = require('run-sequence');

var del = require('del');

var gulp = require('gulp');
var gutil = require('gulp-util');
var filter = require('gulp-filter');

var requireDir = require('require-dir');
var tasks = requireDir('./tasks');

var normalize = require('./plugins/normalize');
var crawlBase = require('./models/crawlBase');
var CrawlState = require('./models/crawlState');
var ParseState = require('./models/parseState');
var config = require('./config/config');

var request = require('request');
var cheerio = require('cheerio');


/**
 * Class to handle fetched content:
 */

var FetchedContent = function(status, headers, content){
  this.status = status;
  this.headers = headers;
  this.content = content;
};

/**
 * Clear the crawl database:
 */

gulp.task('clean:CrawlBase', function (cb){
  del(config.dir.CrawlBase, cb);
});

/**
 * inject: Insert a list of URLs into the crawl database:
 *
 * See:
 *
 *  http://wiki.apache.org/nutch/bin/nutch%20inject
 */

gulp.task('inject', function (){
  return gulp.src(path.join(config.dir.seeds, '*'))

    /**
     * Input is a simple file with a URL per line, so split the file:
     */

    .pipe(through2.obj(function (file, enc, next){
      var self = this;

      file.contents
        .toString()
        .split(/\r?\n/)
        .forEach(function (uri){
          if (uri){
            self.push(normalize(uri));
          }
        });
        next();
    }))

    /**
     * Don't bother if we already have an entry in the crawl database:
     */

    .pipe(es.map(function (uri, cb){
      fs.exists(path.join(config.dir.CrawlBase, encodeURIComponent(uri)),
          function (exists){
        if (exists){
          cb();
        } else {
          cb(null, uri);
        }
      });
    }))

    /**
     * Create a crawl state object for each URL:
     */

    .pipe(es.map(function (uri, cb){
      var file = new gutil.File({
        path: encodeURIComponent(uri)
      });

      file.data = {
        crawlState: new CrawlState()
      };
      cb(null, file);
    }))

    .pipe(crawlBase.dest());
});


/**
 * generate: Place a list of URLs from the crawl database into a crawl list.
 * For now we're only supporting one crawl list:
 *
 * See:
 *
 *  http://wiki.apache.org/nutch/bin/nutch%20generate
 */

gulp.task('generate', function (){
  return crawlBase.src()

    /**
     * Only process data sources that haven't been fetched yet:
     */

    .pipe(filter(function (file){
      return file.data.crawlState.state === CrawlState.UNFETCHED;
    }))

    /**
     * Update the status to indicate that the URL is about to be fetched:
     */

    .pipe(es.map(function (file, cb){
      file.data.crawlState.state = CrawlState.GENERATED;
      file.data.crawlState.retries = 0;
      cb(null, file);
    }))

    /**
     * Update the crawl database with any changes:
     */

    .pipe(crawlBase.dest());
});


/**
 * fetch: Fetch data using a list of URLs:
 *
 * See:
 *
 *  http://wiki.apache.org/nutch/bin/nutch%20fetch
 */

gulp.task('fetch', function (){
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
      var uri = decodeURIComponent(file.relative);

      request(uri, function (err, response, body) {
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
});


/**
 * parse: Parse content from fetched pages:
 *
 * See:
 *
 *  http://wiki.apache.org/nutch/bin/nutch%20parse
 */

gulp.task('parse', function (){
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
            url: url.resolve(decodeURIComponent(file.relative),
              $(this).attr('href') || ''),
            _s: _s,
            title: title
          };
        }).get()
      };
      file.data.parseStatus = new ParseState(ParseState.SUCCESS);
      cb(null, file);
    }))

    /**
     * Update the crawl database with any changes:
     */

    .pipe(crawlBase.dest());
});

/**
 * dbupdate: Updates all rows with inlinks (backlinks), fetchtime and the
 * correct score.
 *
 * See:
 *
 *  https://wiki.apache.org/nutch/Nutch2Crawling#DbUpdate
 */

gulp.task('dbupdate', function (cb){
  runSequence('dbupdate:status', 'dbupdate:outlinks', cb);
});

gulp.task('dbupdate:status', function (){
  return crawlBase.src()

    /**
     * Only process data sources that have been recently fetched:
     */

    .pipe(filter(function (file){
      return (file.data.crawlState.state === CrawlState.GENERATED) &&
        (file.data.fetchedContent);
    }))

    /**
     * Update the crawl state based on the data returned:
     */

    .pipe(es.map(function (file, cb){
      if (file.data.fetchedContent.status === 200){
        file.data.crawlState.state = CrawlState.FETCHED;
      }
      if (file.data.fetchedContent.status === 404 ||
          file.data.crawlState.retries > config.db.fetch.retry.max){
        file.data.crawlState.state = CrawlState.GONE;
      }
      cb(null, file);
    }))

    /**
     * Update the crawl database with any changes:
     */

    .pipe(crawlBase.dest());
});

gulp.task('dbupdate:outlinks', tasks.dbupdate.outlinks);