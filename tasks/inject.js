var path = require('path');

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
  var taskName = 'inject';

  var now = Date.now();

  return gulp.src(config.dir.seeds + path.sep + '*')

    /**
     * Input is a simple file with a URL per line (with optional metdata),
     * so split the file:
     */

    .pipe(through2.obj(function (file, enc, next){
      var self = this;

      file.contents
        .toString()
        .split(/\r?\n/)
        .forEach(function (row){
          if (row && row[0] !== '#'){
            self.push(row);
          }
        });
      next();
    }))

    /**
     * Don't bother if we already have an entry in the crawl database:
     */

    .pipe(through2.obj(function (row, enc, cb){
      var parts = row.split(' ');
      var uri = normalize(parts[0]);

      crawlBase.exists(uri, function (exists){
        if (exists){
          console.info('[%s] skipping \'%s\': already injected', taskName, uri);
          cb();
        } else {
          parts[0] = uri;
          cb(null, parts);
        }
      });
    }))

    /**
     * Create a crawl state object for each row from the input file:
     */

    .pipe(through2.obj(function (row, enc, cb){
      var uri = row.shift();
      var meta = {};

      row.forEach(function(prop) {
        var def = prop.split('=');

        meta[def[0]] = def[1];
      });
      cb(null, crawlBase.crawlState(now, uri, meta));
    }))

    .pipe(through2.obj(function (file, enc, cb){
      console.info('[%s] injecting \'%s\'', taskName, file.data.url);
      cb(null, file);
    }))
    .pipe(crawlBase.dest());
};

module.exports = inject;
