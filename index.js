var runSequence = require('run-sequence');

var del = require('del');

var requireDir = require('require-dir');
var tasks = requireDir('./tasks');

var config = require('./config/config');

var addTasks = function (gulp, crawlBaseTarget, customExtractor, customProcess) {
  var crawlBase = require('./models/crawlBase')(crawlBaseTarget);

  runSequence = runSequence.use(gulp);

  /**
   * Clear the crawl database:
   */

  gulp.task('clean:CrawlBase', function (cb){
    return del(config.dir.CrawlBase, cb);
  });

  /**
   * inject: Insert a list of URLs into the crawl database:
   *
   * See:
   *
   *  https://wiki.apache.org/nutch/Nutch2Crawling#Introduction
   */

  gulp.task('inject', function (cb){
    return tasks.inject(crawlBase, cb);
  });


  /**
   * crawl: Perform a whole cycle of generate, fetch, parse and dbupdate:
   */

  gulp.task('crawl', function (cb){
    return runSequence('generate', 'fetch', 'parse', 'dbupdate', cb);
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
    return tasks.generate(crawlBase, cb);
  });


  /**
   * fetch: Fetch data using a list of URLs:
   *
   * See:
   *
   *  https://wiki.apache.org/nutch/Nutch2Crawling#Fetch
   */

  gulp.task('fetch', function (cb){
    return tasks.fetch(crawlBase, cb);
  });


  /**
   * parse: Parse content from fetched pages:
   *
   * See:
   *
   *  https://wiki.apache.org/nutch/Nutch2Crawling#Parse
   */

  gulp.task('parse', function (cb) {
    return tasks.parse(crawlBase, cb);
  });


  /**
   * extract: Extract :
   */

  gulp.task('extract', function (cb) {
    return tasks.extract(crawlBase, customExtractor, cb);
  });


  /**
   * process: Process :
   */

  gulp.task('process', function (cb) {
    return tasks.process(crawlBase, customProcess, cb);
  });


  /**
   * index: Index content into a search index:
   *
   * See:
   *
   *  https://wiki.apache.org/nutch/bin/nutch_index
   *  https://wiki.apache.org/nutch/bin/nutch%20solrindex
   */

  gulp.task('index', function (cb) {
    return tasks.index(crawlBase, cb);
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
    return runSequence('dbupdate:status', 'dbupdate:outlinks', cb);
  });

  gulp.task('dbupdate:status', function (cb) {
    return tasks.dbupdate.status(crawlBase, cb);
  });
  gulp.task('dbupdate:outlinks', function (cb) {
    return tasks.dbupdate.outlinks(crawlBase, cb);
  });
};

exports.addTasks = addTasks;
