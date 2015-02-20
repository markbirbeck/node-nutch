var fs = require('fs');
var path = require('path');
var url = require('url');

var through2 = require('through2');
var es = require('event-stream');

var gutil = require('gulp-util');
var filter = require('gulp-filter');

var normalize = require('../plugins/normalize');
var crawlBase = require('../models/crawlBase');
var ParseState = require('../models/ParseState');
var CrawlState = require('../models/CrawlState');
var config = require('../config/config');

var status = function (){
  var now = Date.now();

  return crawlBase.src()

    /**
     * Only process data sources that have been recently fetched:
     */

    .pipe(filter(function (file){
      return (file.data.crawlState.state === CrawlState.GENERATED) &&
        (file.data.fetchedContent);
    }))

    /**
     * Update the crawl state based on the data returned:
     */

    .pipe(es.map(function (file, cb){
      if (file.data.fetchedContent.status === 200){
        file.data.crawlState.state = CrawlState.FETCHED;
        file.data.crawlState.fetchTime =
          now + (config.db.fetch.interval.default * 1000);
      }
      if (file.data.fetchedContent.status === 404 ||
          file.data.fetchedContent.status.code === 'ENOTFOUND' ||
          file.data.crawlState.retries > config.db.fetch.retry.max){
        file.data.crawlState.state = CrawlState.GONE;
        file.data.crawlState.fetchTime =
          now + (config.db.fetch.interval.max * 1000);
      }
      cb(null, file);
    }))

    /**
     * Update the crawl database with any changes:
     */

    .pipe(crawlBase.dest());
};

var outlinks = function (){
  return crawlBase.src()

    /**
     * Only update pages with outlinks:
     */

    .pipe(filter(function (file){
      return (file.data.parseStatus &&
        (file.data.parseStatus.state === ParseState.SUCCESS)) &&
        (file.data.parse.outlist.length);
    }))

    /**
     * Check to see if we should ignore outlinks:
     */

    .pipe(filter(function (){
      return config.db.update.additions.allowed;
    }))

    /**
     * Generate an entry for each outlink:
     */

    .pipe(through2.obj(function (file, enc, next){
      var self = this;

      file.data.parse.outlist
        .forEach(function (outlink){
          if (outlink.url){
            var uri = new gutil.File({
              base: config.dir.CrawlBase
            });

            uri.data = {
              inlink: file.data.url
            };
            uri.data.url = outlink.url;
            self.push(uri);
          }
        });
        next();
    }))

    /**
     * Check to see if we should ignore outlinks to external sites:
     */

    .pipe(filter(function (uri){
      if (!config.db.ignore.external.links){
        return true;
      }

      var parsedOutlink = url.parse(uri.data.url);
      var parsedInlink = url.parse(uri.data.inlink);

      return parsedInlink.hostname === parsedOutlink.hostname;
    }))

    /**
     * Normalise the URI so as to reduce possible duplications:
     */

    .pipe(es.map(function (uri, cb){
      uri.data.url = normalize(uri.data.url);
      cb(null, uri);
    }))

    /**
     * Don't bother if we already have an entry in the crawl database:
     */

    .pipe(es.map(function (uri, cb){
      fs.exists(path.join(config.dir.CrawlBase,
          encodeURIComponent(uri.data.url)), function (exists){
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

    .pipe(es.map(function (file, cb){
      file.data.crawlState = new CrawlState();
      cb(null, file);
    }))

    .pipe(crawlBase.dest());
};

exports.status = status;
exports.outlinks = outlinks;