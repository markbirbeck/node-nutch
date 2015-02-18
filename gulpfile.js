var path = require('path');
var Buffer = require('buffer').Buffer;

var es = require('event-stream');
var through2 = require('through2');

var del = require('del');

var gulp = require('gulp');
var gutil = require('gulp-util');


/**
 * Configuration:
 */

var dir = {
  root: 'crawl',
};

/**
 * crawldb: A list of all URLs we know about with their status:
 */

dir.crawldb = path.join(dir.root, 'crawldb');

/**
 * seeds: A set of text files each of which contains URLs:
 */

dir.seeds = path.join(dir.root, 'seeds');


/**
 * Class to handle crawl state:
 */

var CrawlState = function (state){
  this.state = state || CrawlState.UNFETCHED;
};
CrawlState.UNFETCHED = 'unfetched';

/**
 * Clear the crawl database:
 */

gulp.task('clean:crawldb', function (cb){
  del(dir.crawldb, cb);
});


/**
 * inject: Insert a list of URLs into the crawl database:
 *
 * See:
 *
 *  http://wiki.apache.org/nutch/bin/nutch%20inject
 */

gulp.task('inject', ['clean:crawldb'], function (){
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

    .pipe(gulp.dest(dir.crawldb));
});