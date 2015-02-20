var path = require('path');
var fs = require('fs');

var es = require('event-stream');
var through2 = require('through2');
var runSequence = require('run-sequence');

var del = require('del');

var gulp = require('gulp');
var gutil = require('gulp-util');

var requireDir = require('require-dir');
var tasks = requireDir('./tasks');

var normalize = require('./plugins/normalize');
var crawlBase = require('./models/crawlBase');
var CrawlState = require('./models/crawlState');
var config = require('./config/config');


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
 *  https://wiki.apache.org/nutch/Nutch2Crawling#Fetch
 */

gulp.task('fetch', tasks.fetch);


/**
 * parse: Parse content from fetched pages:
 *
 * See:
 *
 *  https://wiki.apache.org/nutch/Nutch2Crawling#Parse
 */

gulp.task('parse', tasks.parse);


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