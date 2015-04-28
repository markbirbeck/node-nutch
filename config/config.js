var path = require('path');

var BasicURLNormalizer = require('../plugins/basicURLNormalizer');

var dir = {
  root: 'crawl'
};

/**
 * CrawlBase: A list of all URLs we know about with their status:
 *
 * TODO: Allow options to be overridden in app that is using node-nutch.
 *
 * To use the file system change the value to:
 *
 *  dir.CrawlBase = dir.root + path.sep + 'CrawlBase';
 */

dir.CrawlBase = 's3://calendar.place/CrawlBase';

/**
 * seeds: A set of text files each of which contains URLs:
 */

dir.seeds = dir.root + path.sep + 'seeds';

var config = {
  dir: dir,
  db: {
    fetch: {
      interval: {

        /**
         * The default number of seconds between re-fetches of a page (30 days):
         */

        default: 2592000,

        /**
         * The maximum number of seconds between re-fetches of a page (90 days).
         * After this period every page in the db will be re-tried, no matter
         * what is its status:
         */

        max: 7776000
      },
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
    },
    update: {
      additions: {

        /**
         * If true, updatedb will add newly discovered URLs, if false
         * only already existing URLs in the CrawlDb will be updated
         * and no new URLs will be added:
         */

        allowed: false
      }
    }
  },

  /**
   * TODO: Allow options to be overridden in app that is using node-nutch.
   */

  elastic: {
    host: 'https://8gpo2qyg:r16yg5bb1pk09vgh@cherry-9017002.us-east-1.bonsai.io',
    log: 'trace'
  },
  urlnormalizer: {

    /**
     * Order in which normalizers will run. If any of these isn't
     * activated it will be silently skipped. If other normalizers
     * not on the list are activated, they will run in random order
     * after the ones specified here are run:
     */

    order: [BasicURLNormalizer]
  }
};

module.exports = config;