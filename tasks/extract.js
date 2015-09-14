var path = require('path');
var _ = require('lodash');

var through2 = require('through2');
var es = require('event-stream');

var filter = require('gulp-filter');

var ExtractState = require('../models/extractState');
var ParseState = require('../models/parseState');
var config = require('../config/config');

var extract = function (crawlBase){
  var taskName = 'extract';
  _.templateSettings.interpolate = /{{([\s\S]+?)}}/g;

  return crawlBase.src()

    /**
     * Only process data sources that have been parsed:
     */

    .pipe(filter(function (file){
      return (
        file.data.parseStatus &&
          (file.data.parseStatus.state === ParseState.SUCCESS) &&
        (!file.data.extractStatus ||
          (file.data.extractStatus &&
          (file.data.extractStatus.state !== ExtractState.SUCCESS))
        )
      );
    }))


    /**
     * Process each line of input:
     */

    .pipe(through2.obj(function (file, enc, next){
      var url = file.data.url;
      var slugTemplate;

      if (file.data.meta) {
        slugTemplate = file.data.meta['slug.template'];
      }
      if (!slugTemplate) {
        slugTemplate = '{{events[0].source}}';
      }

      slugTemplate = _.template(slugTemplate);

      crawlBase.filesSrc(file.data.url, 'parse')
        .pipe(es.map(function(fetchedContent, cb) {
          fetchedContent.base = config.dir.CrawlBase + path.sep;
          fetchedContent.path = config.dir.CrawlBase + path.sep +
            encodeURIComponent(url) + path.sep + 'extract';

          var items = JSON.parse(fetchedContent.contents);

          fetchedContent.contents = new Buffer(JSON.stringify(
            items.map(function(item) {
              var params = {summary: item.summary};

              item.slug = slugTemplate(params).replace(/ /g, '-');
              item.url = url;
              return item;
            })
          ));
          file.data.extractStatus = new ExtractState(ExtractState.SUCCESS);
          cb(null, fetchedContent);
        }))
        .pipe(crawlBase.filesDest())
        .on('end', function() {
          next(null, file);
        });
    }))

    .pipe(through2.obj(function (file, enc, next){
      console.info(
        '[%s] extracted \'%s\'',
        taskName,
        file.relative
      );
      next(null, file);
    }))

    /**
     * Update the crawl database with any changes:
     */

    .pipe(crawlBase.dest())
    ;
};

module.exports = extract;
