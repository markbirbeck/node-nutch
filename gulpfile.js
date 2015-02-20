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

var cheerio = require('cheerio');

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
        base: config.dir.CrawlBase
      });

      file.data = {
        crawlState: new CrawlState()
      };
      file.data.url = uri;
      cb(null, file);
    }))

    .pipe(crawlBase.dest());
});


/**
 * generate: Creates a new batch. Selects urls to fetch from the webtable
 * (or: marks urls in the webtable which need to be fetched).
 *
 * For now we're only supporting one batch:
 *
 * See:
 *
 *  https://wiki.apache.org/nutch/Nutch2Crawling#Generate
 */

gulp.task('generate', tasks.generate);


/**
 * fetch: Fetch data using a list of URLs:
 *
 * See:
 *
 *  http://wiki.apache.org/nutch/bin/nutch%20fetch
 */

gulp.task('fetch', tasks.fetch);


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

gulp.task('dbupdate:status', tasks.dbupdate.status);
gulp.task('dbupdate:outlinks', tasks.dbupdate.outlinks);