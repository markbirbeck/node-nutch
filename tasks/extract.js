var gutil = require('gulp-util');

var es = require('event-stream');
var through2 = require('through2');

var filter = require('gulp-filter');

var elasticsearch = require('vinyl-elasticsearch');
var uuid = require('uuid');

var crawlBase = require('../models/crawlBase');
var ParseState = require('../models/parseState');

var extract = function (customExtractor, customStore){
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
     * Store each line of input:
     */

    // .pipe(
    //   elasticsearch.dest({
    //     index: 'calendar.place',
    //     type: 'event',
    //   }, {
    //     host: 'https://8gpo2qyg:r16yg5bb1pk09vgh@cherry-9017002.us-east-1.bonsai.io',
    //     log: 'trace'
    //   })
    // )
    .pipe(customStore())

    /**
     * Update the crawl database with any changes:
     */

    // .pipe(crawlBase.dest())
    ;
};

module.exports = extract;
