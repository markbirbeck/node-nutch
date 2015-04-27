var runSequence = require('run-sequence');

var del = require('del');

var requireDir = require('require-dir');
var tasks = requireDir('./tasks');

var config = require('./config/config');

var crawlBase = require('./models/crawlBase')();

var addTasks = function (gulp, customParser, customExtractor, customStore){

  /**
   * Clear the crawl database:
   */

  gulp.task('clean:CrawlBase', function (cb){
    del(config.dir.CrawlBase, cb);
  });

  /**
   * inject: Insert a list of URLs into the crawl database:
   *
   * See:
   *
   *  https://wiki.apache.org/nutch/Nutch2Crawling#Introduction
   */

  gulp.task('inject', function (cb){
    tasks.inject(crawlBase, cb);
  });


  /**
   * crawl: Perform a whole cycle of generate, fetch, parse and dbupdate:
   */

  gulp.task('crawl', function (cb){
    runSequence('generate', 'fetch', 'parse', 'dbupdate', cb).use(gulp);
  });


  /**
   * generate: Creates a new batch. Selects urls to fetch from the webtable
   * (or: marks urls in the webtable which need to be fetched).
   *
   * For now we're only supporting one batch:
   *
   * See:
   *
   *  https://wiki.apache.org/nutch/Nutch2Crawling#Generate
   */

  gulp.task('generate', function (cb){
    tasks.generate(crawlBase, cb);
  });


  /**
   * fetch: Fetch data using a list of URLs:
   *
   * See:
   *
   *  https://wiki.apache.org/nutch/Nutch2Crawling#Fetch
   */

  gulp.task('fetch', function (cb){
    tasks.fetch(crawlBase, cb);
  });


  /**
   * parse: Parse content from fetched pages:
   *
   * See:
   *
   *  https://wiki.apache.org/nutch/Nutch2Crawling#Parse
   */

  gulp.task('parse', function () {
    return tasks.parse(crawlBase, customParser);
  });

  gulp.task('extract', function () {
    return tasks.extract(crawlBase, customExtractor, customStore);
  });

  /**
   * dbupdate: Updates all rows with inlinks (backlinks), fetchtime and the
   * correct score.
   *
   * See:
   *
   *  https://wiki.apache.org/nutch/Nutch2Crawling#DbUpdate
   */

  gulp.task('dbupdate', function (cb){
    runSequence('dbupdate:status', 'dbupdate:outlinks', cb).use(gulp);
  });

  gulp.task('dbupdate:status', function () {
    return tasks.dbupdate.status(crawlBase);
  });
  gulp.task('dbupdate:outlinks', function () {
    return tasks.dbupdate.outlinks(crawlBase);
  });
};

exports.addTasks = addTasks;
