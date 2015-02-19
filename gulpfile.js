var path = require('path');
var Buffer = require('buffer').Buffer;

var es = require('event-stream');
var through2 = require('through2');
var lazypipe = require('lazypipe');

var del = require('del');

var gulp = require('gulp');
var gutil = require('gulp-util');
var filter = require('gulp-filter');

var request = require('request');

/**
 * Configuration:
 */

var dir = {
  root: 'crawl'
};
var MAX_RETRIES = 3;

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

gulp.task('inject', ['clean:CrawlBase'], function (){
  return gulp.src(path.join(dir.seeds, '*'))

    /**
     * Input is a simple file with a URL per line, so split the file:
     */

    .pipe(through2.obj(function (file, enc, next){
      var self = this;

      file.contents
        .toString()
        .split(/\r?\n/)
        .forEach(function (url){
          self.push(url);
        });
        next();
    }))


    /**
     * Create a crawl state object for each URL:
     */

    .pipe(es.map(function (url, cb){
      var file = new gutil.File({
        path: encodeURIComponent(url)
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
      var url = decodeURIComponent(file.relative);

      request(url, function (err, response, body) {
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
 * updatedb: Update the crawl database with the results of a fetch:
 *
 * See:
 *
 *  http://wiki.apache.org/nutch/bin/nutch%20updatedb
 */

gulp.task('updatedb', function (){
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
      if (file.data.fetchedContent.status === 404 || file.data.crawlState.retries > MAX_RETRIES){
        file.data.crawlState.state = CrawlState.GONE;
      }
      cb(null, file);
    }))

    /**
     * Update the crawl database with any changes:
     */

    .pipe(crawlBase.dest());
});