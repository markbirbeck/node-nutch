var runSequence = require('run-sequence');

var del = require('del');

var gulp = require('gulp');

var requireDir = require('require-dir');
var tasks = requireDir('./tasks');

var config = require('./config/config');


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

gulp.task('inject', tasks.inject);


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

gulp.task('generate', tasks.generate);


/**
 * fetch: Fetch data using a list of URLs:
 *
 * See:
 *
 *  https://wiki.apache.org/nutch/Nutch2Crawling#Fetch
 */

gulp.task('fetch', tasks.fetch);


/**
 * parse: Parse content from fetched pages:
 *
 * See:
 *
 *  https://wiki.apache.org/nutch/Nutch2Crawling#Parse
 */

gulp.task('parse', tasks.parse);


/**
 * dbupdate: Updates all rows with inlinks (backlinks), fetchtime and the
 * correct score.
 *
 * See:
 *
 *  https://wiki.apache.org/nutch/Nutch2Crawling#DbUpdate
 */

gulp.task('dbupdate', function (cb){
  runSequence('dbupdate:status', 'dbupdate:outlinks', cb);
});

gulp.task('dbupdate:status', tasks.dbupdate.status);
gulp.task('dbupdate:outlinks', tasks.dbupdate.outlinks);