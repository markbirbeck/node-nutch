var _ = require('lodash');

var through2 = require('through2');

var filter = require('gulp-filter');

var ExtractState = require('../models/extractState');
var ParseState = require('../models/parseState');

var extract = function (crawlBase, customExtractor){
  var taskName = 'extract';
  _.templateSettings.interpolate = /{{([\s\S]+?)}}/g;

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
      var url = file.data.url;
      var slugTemplate;

      if (file.data.meta) {
        slugTemplate = file.data.meta['slug.template'];
      }
      if (!slugTemplate) {
        slugTemplate = '{{events[0].source}}';
      }

      slugTemplate = _.template(slugTemplate);

      file.data.extracted = customExtractor(
          file.data.customParseChanged || file.data.customParse)
        .filter(function (obj){
          var params = {};

          if (!obj || !obj.events){
            return false;
          }

          obj.url = url;

          params.summary = obj.summary || obj.events[0].summary;
          if (obj.events[0].timex3.date) {
            params.year = obj.events[0].timex3.date.year;
            params.month = obj.events[0].timex3.date.month;
          } else if (obj.events[0].timex3.range) {
            params.year = obj.events[0].timex3.range.from.date.year;
            params.month = obj.events[0].timex3.range.from.date.month;
          }

          try {
            obj.slug = slugTemplate(params).replace(/ /g, '-');
            return true;
          } catch (e) {
            console.error('Template processing failed:', e,
              JSON.stringify(obj));
          }
          return false;
        });
      next(null, file);
    }))

    .pipe(through2.obj(function (file, enc, next){
      file.data.extractStatus = new ExtractState(ExtractState.SUCCESS);
      console.info('[%s] extracted \'%s\' (source: "%s")', taskName,
        file.relative, JSON.stringify(file.data.extracted));
      next(null, file);
    }))

    /**
     * Update the crawl database with any changes:
     */

    .pipe(crawlBase.dest())
    ;
};

module.exports = extract;
