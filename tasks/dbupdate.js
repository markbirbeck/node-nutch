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
     * Generate an entry for each outlink:
     */

    .pipe(through2.obj(function (file, enc, next){
      var self = this;

      file.data.parse.outlist
        .forEach(function (outlink){
          if (outlink.url){
            var uri = new gutil.File({
              path: encodeURIComponent(outlink.url)
            });

            uri.data = {
              inlink: decodeURIComponent(file.relative)
            };
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

      var parsedOutlink = url.parse(decodeURIComponent(uri.relative));
      var parsedInlink = url.parse(uri.data.inlink);

      return parsedInlink.hostname === parsedOutlink.hostname;
    }))

    /**
     * Normalise the URI so as to reduce possible duplications:
     */

    .pipe(es.map(function (uri, cb){
      uri.path = normalize(uri.relative);
      cb(null, uri);
    }))

    /**
     * Don't bother if we already have an entry in the crawl database:
     */

    .pipe(es.map(function (uri, cb){
      fs.exists(path.join(config.dir.CrawlBase, uri.relative),
          function (exists){
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
      file.data = {
        crawlState: new CrawlState()
      };
      cb(null, file);
    }))

    .pipe(crawlBase.dest());
};

exports.outlinks = outlinks;