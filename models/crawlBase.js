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

module.exports = function(target) {
  /**
   * If there is no output target specified then use Gulp's default:
   */

  if (!target) {
    target = gulp;
    gulp.exists = fs.exists;
  }

  return {
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
        file.contentType = 'application/json';
        file.base = config.dir.CrawlBase;
        file.path = config.dir.CrawlBase + path.sep + encodeURIComponent(file.data.url);
        cb(null, file);
      })
      .pipe(target.dest, config.dir.CrawlBase),
    exists: function(uri, cb){
      target.exists(config.dir.CrawlBase + path.sep + encodeURIComponent(uri), cb);
    },
    src: function (){
      return target.src(config.dir.CrawlBase + '/*')
        .pipe(es.map(function (file, cb){
          file.data = JSON.parse(file.contents.toString());
          file.path = file.data.url;

          cb(null, file);
        }));
    }
  };
};