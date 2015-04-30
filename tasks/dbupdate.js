var path = require('path');
var url = require('url');

var through2 = require('through2');
var es = require('event-stream');

var gutil = require('gulp-util');
var filter = require('gulp-filter');

var normalize = require('../plugins/normalize');
var ParseState = require('../models/parseState');
var CrawlState = require('../models/crawlState');
var config = require('../config/config');

var status = function (crawlBase){
  var taskName = 'dbupdate:status';
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
      }
      if (file.data.fetchedContent.status === 404 ||
          file.data.fetchedContent.status.code === 'ENOTFOUND' ||
          file.data.crawlState.retries > config.db.fetch.retry.max){
        file.data.crawlState.state = CrawlState.GONE;
      }
      cb(null, file);
    }))

    /**
     * Update the fetch time based on the status:
     */

    .pipe(es.map(function (file, cb){
      var offset;

      switch (file.data.crawlState.state){
        case CrawlState.FETCHED:
          offset = config.db.fetch.interval.default;
          break;

        case CrawlState.GONE:
          offset = config.db.fetch.interval.max;
          break;

        default:
          offset = 0;
          break;
      }
      file.data.crawlState.fetchTime = now + (offset * 1000);
      cb(null, file);
    }))

    /**
     * Update the crawl database with any changes:
     */

    .pipe(through2.obj(function (file, enc, cb){
      console.info('[%s] \'%s\' marked as %s', taskName, file.data.url,
        file.data.crawlState.state);
      cb(null, file);
    }))

    .pipe(crawlBase.dest());
};

var outlinks = function (crawlBase){
  var taskName = 'dbupdate:outlinks';
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
      crawlBase.exists(config.dir.CrawlBase + path.sep + encodeURIComponent(uri.data.url), function (exists){
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

    .pipe(through2.obj(function (file, enc, cb){
      console.info('[%s] injecting \'%s\'', taskName, file.data.url);
      cb(null, file);
    }))
    .pipe(crawlBase.dest());
};

exports.status = status;
exports.outlinks = outlinks;