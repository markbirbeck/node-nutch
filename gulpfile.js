var path = require('path');
var Buffer = require('buffer').Buffer;
var url = require('url');
var fs = require('fs');

var es = require('event-stream');
var through2 = require('through2');
var lazypipe = require('lazypipe');
var runSequence = require('run-sequence');

var del = require('del');

var gulp = require('gulp');
var gutil = require('gulp-util');
var filter = require('gulp-filter');

var request = require('request');
var cheerio = require('cheerio');
var URI = require('URIjs');

/**
 * Configuration:
 */

var dir = {
  root: 'crawl'
};

var BasicURLNormalizer = function (){};
BasicURLNormalizer.prototype.normalize = function(urlString){
  return URI(urlString)
    .fragment('')
    .normalize()
    .toString();
};

var config = {
  db: {
    fetch: {
      retry: {

        /**
         * The maximum number of times a url that has encountered
         * recoverable errors is generated for fetch:
         */

        max: 3
      }
    },
    ignore: {
      external: {

        /**
         * If true, outlinks leading from a page to external hosts
         * will be ignored. This is an effective way to limit the
         * crawl to include only initially injected hosts, without
         * creating complex URLFilters:
         */

        links: true
      }
    }
  },
  urlnormalizer: {

    /**
     * Order in which normalizers will run. If any of these isn't
     * activated it will be silently skipped. If other normalizers
     * not on the list are activated, they will run in random order
     * after the ones specified here are run:
     */

    order: [new BasicURLNormalizer()]
  }
};

var normalize = function (urlString){
  return config.urlnormalizer.order.reduce(function (value, normalizer){
    return normalizer.normalize(value);
  }, urlString);
};

/**
 * CrawlBase: A list of all URLs we know about with their status:
 */

dir.CrawlBase = path.join(dir.root, 'CrawlBase');

/**
 * seeds: A set of text files each of which contains URLs:
 */

dir.seeds = path.join(dir.root, 'seeds');


/**
 * Class to handle crawl state:
 */

var CrawlState = function (state){
  this.state = state || CrawlState.UNFETCHED;
};
CrawlState.UNFETCHED = 'unfetched';
CrawlState.GENERATED = 'generated';
CrawlState.FETCHED = 'fetched';
CrawlState.GONE = 'gone';

/**
 * Class to handle fetched content:
 */

var FetchedContent = function(status, headers, content){
  this.status = status;
  this.headers = headers;
  this.content = content;
};

/**
 * Class to handle parse state:
 */

var ParseState = function (state){
  this.state = state || ParseState.NOTPARSED;
};
ParseState.NOTPARSED = 'notparsed';
ParseState.SUCCESS = 'success';
ParseState.FAILED = 'failed';

/**
 * Clear the crawl database:
 */

gulp.task('clean:CrawlBase', function (cb){
  del(dir.CrawlBase, cb);
});

/**
 * Create a pipeline to update the crawl database with any changes to an entry:
 */

var crawlBase = {
  dest: lazypipe()
    .pipe(es.map, function (file, cb){
      file.contents = new Buffer(JSON.stringify( file.data ));
      cb(null, file);
    })
    .pipe(gulp.dest, dir.CrawlBase),
  src: function (){
    return gulp.src(path.join(dir.CrawlBase, '*'))
      .pipe(es.map(function (file, cb){
        file.data = JSON.parse(file.contents.toString());

        cb(null, file);
      }));
  }
};

/**
 * inject: Insert a list of URLs into the crawl database:
 *
 * See:
 *
 *  http://wiki.apache.org/nutch/bin/nutch%20inject
 */

gulp.task('inject', function (){
  return gulp.src(path.join(dir.seeds, '*'))

    /**
     * Input is a simple file with a URL per line, so split the file:
     */

    .pipe(through2.obj(function (file, enc, next){
      var self = this;

      file.contents
        .toString()
        .split(/\r?\n/)
        .forEach(function (uri){
          if (uri){
            self.push(normalize(uri));
          }
        });
        next();
    }))

    /**
     * Don't bother if we already have an entry in the crawl database:
     */

    .pipe(es.map(function (uri, cb){
      fs.exists(path.join(dir.CrawlBase, encodeURIComponent(uri)), function (exists){
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

    .pipe(es.map(function (uri, cb){
      var file = new gutil.File({
        path: encodeURIComponent(uri)
      });

      file.data = {
        crawlState: new CrawlState()
      };
      cb(null, file);
    }))

    .pipe(crawlBase.dest());
});


/**
 * generate: Place a list of URLs from the crawl database into a crawl list.
 * For now we're only supporting one crawl list:
 *
 * See:
 *
 *  http://wiki.apache.org/nutch/bin/nutch%20generate
 */

gulp.task('generate', function (){
  return crawlBase.src()

    /**
     * Only process data sources that haven't been fetched yet:
     */

    .pipe(filter(function (file){
      return file.data.crawlState.state === CrawlState.UNFETCHED;
    }))

    /**
     * Update the status to indicate that the URL is about to be fetched:
     */

    .pipe(es.map(function (file, cb){
      file.data.crawlState.state = CrawlState.GENERATED;
      file.data.crawlState.retries = 0;
      cb(null, file);
    }))

    /**
     * Update the crawl database with any changes:
     */

    .pipe(crawlBase.dest());
});


/**
 * fetch: Fetch data using a list of URLs:
 *
 * See:
 *
 *  http://wiki.apache.org/nutch/bin/nutch%20fetch
 */

gulp.task('fetch', function (){
  return crawlBase.src()

    /**
     * Only process data sources that are ready to be fetched:
     */

    .pipe(filter(function (file){
      return file.data.crawlState.state === CrawlState.GENERATED;
    }))

    /**
     * Retrieve the document from the URL:
     */

    .pipe(es.map(function (file, cb){
      var uri = decodeURIComponent(file.relative);

      request(uri, function (err, response, body) {
        var headers;
        var status;

        if (err){
          status = err;
          headers = '';
          body = '';
        } else {
          status = response.statusCode;
          headers = response.headers;
        }

        file.data.fetchedContent = new FetchedContent(status, headers, body);
        file.data.crawlState.retries++;
        cb(null, file);
      });
    }))

    /**
     * Update the crawl database with any changes:
     */

    .pipe(crawlBase.dest());
});


/**
 * parse: Parse content from fetched pages:
 *
 * See:
 *
 *  http://wiki.apache.org/nutch/bin/nutch%20parse
 */

gulp.task('parse', function (){
  return crawlBase.src()

    /**
     * Only process data sources that have been fetched, and not parsed:
     */

    .pipe(filter(function (file){
      return (file.data.fetchedContent.status === 200) &&
        (!file.data.parseStatus || file.data.parseStatus.state === ParseState.NOTPARSED);
    }))

    /**
     * Update the status to indicate that the URL is about to be fetched:
     */

    .pipe(es.map(function (file, cb){
      var $ = cheerio.load(file.data.fetchedContent.content);
      file.data.parse = {
        title: $('title').text().trim(),
        outlist: $('a').map(function (){
          var title;
          var _s;

          if ($(this).attr('title')){
            _s = '@title';
            title = $(this).attr('title');
          } else {
            title = $(this).text().trim();
            _s = 'text';
            if (title === ''){
              if ($('img', this)){
                _s = 'img[@alt]';
                title = $('img', this).attr('alt');
              }
            }
          }

          return {
            url: url.resolve(decodeURIComponent(file.relative), $(this).attr('href') || ''),
            _s: _s,
            title: title
          };
        }).get()
      };
      file.data.parseStatus = new ParseState(ParseState.SUCCESS);
      cb(null, file);
    }))

    /**
     * Update the crawl database with any changes:
     */

    .pipe(crawlBase.dest());
});

/**
 * updatedb: Update the crawl database with the results of a fetch:
 *
 * See:
 *
 *  http://wiki.apache.org/nutch/bin/nutch%20updatedb
 */

gulp.task('updatedb', function (cb){
  runSequence('updatedb:status', 'updatedb:outlinks', cb);
});

gulp.task('updatedb:status', function (){
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
      if (file.data.fetchedContent.status === 404 || file.data.crawlState.retries > config.db.fetch.retry.max){
        file.data.crawlState.state = CrawlState.GONE;
      }
      cb(null, file);
    }))

    /**
     * Update the crawl database with any changes:
     */

    .pipe(crawlBase.dest());
});

gulp.task('updatedb:outlinks', function (){
  return crawlBase.src()

    /**
     * Only update pages with outlinks:
     */

    .pipe(filter(function (file){
      return (file.data.parseStatus && (file.data.parseStatus.state === ParseState.SUCCESS)) &&
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
      if (!config.db.ignore.external.links)
        return true;

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
      fs.exists(path.join(dir.CrawlBase, uri.relative), function (exists){
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
});