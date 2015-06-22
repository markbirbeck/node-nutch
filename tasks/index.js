var _ = require('lodash');
var gutil = require('gulp-util');

var es = require('event-stream');
var through2 = require('through2');

var filter = require('gulp-filter');

var elasticsearch = require('vinyl-elasticsearch');

var ExtractState = require('../models/extractState');

var config = require('../config/config');

var index = function (crawlBase){
  var taskName = 'index';
  _.templateSettings.interpolate = /{{([\s\S]+?)}}/g;

  return crawlBase.src()

    /**
     * Process extracted data:
     */

    .pipe(filter(function (file){
      return (file.data.extractStatus &&
        (file.data.extractStatus.state === ExtractState.SUCCESS));
    }))


    /**
     * Process each line of input:
     */

    .pipe(through2.obj(function (file, enc, next){
      var self = this;

      file.data.extracted.forEach(function (obj){
        self.push(obj);
      });
      next();
    }))


    /**
     * Turn into a Vinyl file:
     */

    .pipe(es.map(function (obj, cb){
      if (!obj.slug) {
        cb(new Error(
          'No slug. Please check there is a slug rule for this site.'));
      } else {
        var file = new gutil.File({
          path: obj.slug
        });

        file.data = obj;
        cb(null, file);
      }
    }))


    /**
     * Provide an id for ElasticSearch:
     */

    .pipe(through2.obj(function (file, enc, cb){
      file.id = file.relative;
      cb(null, file);
    }))

    /**
     * Store each line of input:
     */

    .pipe(elasticsearch.dest({

        /**
         * [TODO] Sort out configuration.
         */

        index: 'calendar.place',
        type: 'event'
      },
      config.elastic
    ))
    .pipe(through2.obj(function (file, enc, cb){
      console.info('[%s] indexed \'%s\' (source: "%s")', taskName,
        file.relative, JSON.stringify(file.data));
      cb(null, file);
    }))

    /**
     * Update the crawl database with any changes:
     */

    // .pipe(crawlBase.dest())
    ;
};

module.exports = index;
