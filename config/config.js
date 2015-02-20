var path = require('path');

var BasicURLNormalizer = require('../plugins/basicURLNormalizer');

var dir = {
  root: 'crawl'
};

/**
 * CrawlBase: A list of all URLs we know about with their status:
 */

dir.CrawlBase = path.join(dir.root, 'CrawlBase');

/**
 * seeds: A set of text files each of which contains URLs:
 */

dir.seeds = path.join(dir.root, 'seeds');

var config = {
  dir: dir,
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