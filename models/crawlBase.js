var Buffer = require('buffer').Buffer;
var path = require('path');
var es = require('event-stream');
var lazypipe = require('lazypipe');

var gulp = require('gulp');

var config = require('../config/config');

/**
 * Create a pipeline to update the crawl database with any changes to an entry:
 */

module.exports = {
  dest: lazypipe()
    .pipe(es.map, function (file, cb){
      file.contents = new Buffer(JSON.stringify( file.data ));
      cb(null, file);
    })
    .pipe(gulp.dest, config.dir.CrawlBase),
  src: function (){
    return gulp.src(path.join(config.dir.CrawlBase, '*'))
      .pipe(es.map(function (file, cb){
        file.data = JSON.parse(file.contents.toString());

        cb(null, file);
      }));
  }
};
