var Buffer = require('buffer').Buffer;
var fs = require('fs');
var path = require('path');
var es = require('event-stream');
var lazypipe = require('lazypipe');

var gulp = require('gulp');
var gutil = require('gulp-util');

var CrawlState = require('./crawlState');
var config = require('../config/config');

/**
 * Create a pipeline to update the crawl database with any changes to an entry:
 */

module.exports = {
  crawlState: function(t, uri){
    var file = new gutil.File({
      base: config.dir.CrawlBase
    });

    file.data = {
      crawlState: new CrawlState(t)
    };
    file.data.url = uri;
    return file;
  },
  dest: lazypipe()
    .pipe(es.map, function (file, cb){
      file.contents = new Buffer(JSON.stringify( file.data ));
      file.path = path.join(config.dir.CrawlBase,
        encodeURIComponent(file.data.url));
      cb(null, file);
    })
    .pipe(gulp.dest, config.dir.CrawlBase),
  exists: function(uri, cb){
    fs.exists(path.join(config.dir.CrawlBase, encodeURIComponent(uri)), cb);
  },
  src: function (){
    return gulp.src(path.join(config.dir.CrawlBase, '*'))
      .pipe(es.map(function (file, cb){
        file.data = JSON.parse(file.contents.toString());

        cb(null, file);
      }));
  }
};
