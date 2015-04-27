var path = require('path');

var es = require('event-stream');
var through2 = require('through2');

var gulp = require('gulp');

var normalize = require('../plugins/normalize');

var config = require('../config/config');

/**
 * inject: Insert a list of URLs into the crawl database:
 *
 * See:
 *
 *  https://wiki.apache.org/nutch/Nutch2Crawling#Introduction
 */

var inject = function (crawlBase){
  var now = Date.now();

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
          if (uri && uri[0] !== '#'){
            self.push(normalize(uri));
          }
        });
      next();
    }))

    /**
     * Don't bother if we already have an entry in the crawl database:
     */

    .pipe(es.map(function (uri, cb){
      crawlBase.exists(uri, function (exists){
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
      cb(null, crawlBase.crawlState(now, uri));
    }))

    .pipe(crawlBase.dest());
};

module.exports = inject;
