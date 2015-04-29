var gutil = require('gulp-util');

var es = require('event-stream');
var through2 = require('through2');

var filter = require('gulp-filter');

var elasticsearch = require('vinyl-elasticsearch');
var uuid = require('uuid');

var ParseState = require('../models/parseState');

var config = require('../config/config');

var index = function (crawlBase, customExtractor){
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
      cb(
        null,
        new gutil.File({
          contents: new Buffer(JSON.stringify(obj)),
          path: uuid.v1()
        })
      );
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

    /**
     * Update the crawl database with any changes:
     */

    // .pipe(crawlBase.dest())
    ;
};

module.exports = index;
