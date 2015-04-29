var gutil = require('gulp-util');

var es = require('event-stream');
var through2 = require('through2');

var filter = require('gulp-filter');

var elasticsearch = require('vinyl-elasticsearch');
var uuid = require('uuid');

var ParseState = require('../models/parseState');

var config = require('../config/config');

var index = function (crawlBase, customExtractor){
  var taskName = 'index';

  return crawlBase.src()

    /**
     * Only process data sources that have been parsed:
     */

    .pipe(filter(function (file){
      return (file.data.customParseStatus &&
        (file.data.customParseStatus.state === ParseState.SUCCESS));
    }))


    /**
     * Process each line of input:
     */

    .pipe(through2.obj(function (file, enc, next){
      var self = this;
      var url = file.data.url;

      file.data.customParse
        .forEach(function (row){
          var obj = customExtractor(row);

          if (obj){
            obj.source = row;
            obj.url = url;
            self.push(obj);
          }
        });
      next();
    }))


    /**
     * Turn into a Vinyl file:
     */

    .pipe(es.map(function (obj, cb){
      var file = new gutil.File({
        path: uuid.v1()
      });

      file.data = obj;
      cb(null, file);
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
        type: 'event',
      },
      config.elastic
    ))
    .pipe(through2.obj(function (file, enc, cb){
      console.info('[%s] indexed \'%s\' (source: "%s")', taskName,
        file.relative, file.data.source);
      cb(null, file);
    }))

    /**
     * Update the crawl database with any changes:
     */

    // .pipe(crawlBase.dest())
    ;
};

module.exports = index;
