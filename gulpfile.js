var path = require('path');
var Buffer = require('buffer').Buffer;

var es = require('event-stream');
var through2 = require('through2');

var del = require('del');

var gulp = require('gulp');
var gutil = require('gulp-util');
var concat = require('gulp-concat');
var filter = require('gulp-filter');


/**
 * Configuration:
 */

var dir = {
  root: 'crawl',
  CrawlList1: '1.txt'
};

/**
 * CrawlBase: A list of all URLs we know about with their status:
 */

dir.CrawlBase = path.join(dir.root, 'CrawlBase');

/**
 * seeds: A set of text files each of which contains URLs:
 */

dir.seeds = path.join(dir.root, 'seeds');

/**
 * CrawlList: A crawl list is a set of URLs that will be processed together:
 */

dir.CrawlList = path.join(dir.root, 'CrawlList');


/**
 * Class to handle crawl state:
 */

var CrawlState = function (state){
  this.state = state || CrawlState.UNFETCHED;
};
CrawlState.UNFETCHED = 'unfetched';
CrawlState.GENERATED = 'generated';

/**
 * Clear the crawl database:
 */

gulp.task('clean:CrawlBase', function (cb){
  del(dir.CrawlBase, cb);
});


/**
 * inject: Insert a list of URLs into the crawl database:
 *
 * See:
 *
 *  http://wiki.apache.org/nutch/bin/nutch%20inject
 */

gulp.task('inject', ['clean:CrawlBase'], function (){
  return gulp.src(path.join(dir.seeds, '*'))

    /**
     * Input is a simple file with a URL per line, so split the file:
     */

    .pipe(through2.obj(function (file, enc, next){
      var self = this;

      file.contents
        .toString()
        .split(/\r?\n/)
        .forEach(function (url){
          self.push(url);
        });
        next();
    }))


    /**
     * Create a crawl state object for each URL:
     */

    .pipe(es.map(function (url, cb){
      var crawlState = new CrawlState();
      var file = new gutil.File({
        path: encodeURIComponent(url),
        contents: new Buffer(JSON.stringify( crawlState ))
      });

      cb(null, file);
    }))

    .pipe(gulp.dest(dir.CrawlBase));
});


/**
 * Clear all crawl lists:
 */

gulp.task('clean:CrawlList', function (cb){
  del(dir.CrawlList, cb);
});

/**
 * Clear crawl list 1:
 */

gulp.task('clean:CrawlList1', function (cb){
  del(path.join(dir.CrawlList, dir.CrawlList1), cb);
});


/**
 * generate: Place a list of URLs from the crawl database into a crawl list.
 * For now we're only supporting one crawl list:
 *
 * See:
 *
 *  http://wiki.apache.org/nutch/bin/nutch%20generate
 */

gulp.task('generate', ['clean:CrawlList1'], function (){
  return gulp.src(path.join(dir.CrawlBase, '*'))

    /**
     * Save a bit of processing by only parsing the JSON once:
     */

    .pipe(es.map(function (file, cb){
      file.crawlState = JSON.parse(file.contents.toString());

      cb(null, file);
    }))

    /**
     * Only process data sources that haven't been fetched yet:
     */

    .pipe(filter(function (file){
      return file.crawlState.state === CrawlState.UNFETCHED;
    }))

    /**
     * Update the status to indicate that the URL is about to be fetched:
     */

    .pipe(es.map(function (file, cb){
      file.crawlState.state = CrawlState.GENERATED;
      cb(null, file);
    }))

    /**
     * Update the crawl database with any changes:
     */

    .pipe(es.map(function (file, cb){
      file.contents = new Buffer(JSON.stringify( file.crawlState ));
      cb(null, file);
    }))
    .pipe(gulp.dest(dir.CrawlBase))

    /**
     * Finally, create a crawl list:
     */

    .pipe(es.map(function (file, cb){
      file.contents = new Buffer(decodeURIComponent(file.relative));
      cb(null, file);
    }))
    .pipe(concat(dir.CrawlList1))
    .pipe(gulp.dest(dir.CrawlList));
});
