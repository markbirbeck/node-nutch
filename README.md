node-nutch
==========

A set of Gulp commands that provide similar functionality to [Apache Nutch](http://nutch.apache.org/).

Whilst Nutch was used for inspiration, as things have developed the reasons for following Nutch patterns have become fewer and fewer, so a lot will probably change; at some point this project will be factored so that it becomes a set of [sifttt](https://www.npmjs.com/package/sifttt) recipes.

# Concepts

## Design

### The Nutch Model

Nutch is designed to have a number of tasks that can be run in a self-contained way, and the status of the tasks stored centrally. This makes it possible to run many servers, each running different parts of the pipeline, at different stages.

### Other Node Crawlers

There are lots of Node crawlers but generally they retrieve documents and then process those documents all in one step. This makes it difficult to scale them, which is why we've followed Nutch's model. [bot-marvin](https://www.npmjs.com/package/bot-marvin) is an honurable exception, but seems too closely integrated with MongoDB at the moment.

Some Node crawlers can be enhanced by adding extra pipelines (such as [roboto](https://www.npmjs.com/package/roboto)) but we feel that there is already a well established pipeline model in the Node eco-system, based on Vinyl files and Gulp streams, so we have decided to integrate with that.

## CrawlBase

Crawled files and their status are stored in a _CrawlBase_ which eventually could be located anywhere that can be a Gulp destination. So far the targets that have been used reliably are the file system (via Gulp) and S3, but in principle the _CrawlBase_ could be located in a relational database, ElasticSearch, or whatever.

The path to the _CrawlBase_ defaults to `crawl/Crawlbase`, and can be set with the environment variable `CRAWLBASE`.

## Status

Each command does its work and then sets some indication of status in the `status` file. This is then used by subsequent commands to determine what needs doing.

# Commands

E.g.:

    gulp inject

## clean:CrawlBase

Completely deletes the crawl database.

## inject

Inject some URLs into the crawl database, ready for crawling. The list of URLs to inject will come from files in the `seeds` directory. All files are read and each line in the file is processed, so there can be as many or few files as needed. Any line that begins with a '#' is ignored.

## generate

To indicate that a URL is ready to be fetched, run `generate`. At the moment this will cause _all_ URLs to be made candidates for fetching, but in the future this may become more refined, and a subset of the URLs could be chosen.

## fetch

The `fetch` command will retrieve all URLs and update the status with the return code, headers and of course the body of the document retrieved.

## parse

The `parse` command will process the retrieved document and convert it to the desired format.

## extract

Sometimes only a part of a document is required, particularly if an API is being crawled. The `extract` command will convert the parse document to some further document.

## process

The commands above are used to handle the crawling and recrawling, but the `process` command is used to do any basic processing with the extracted data, ready to be used by some other processor.

## dbupdate:status

The `dbupdate:status` command will update various values in the `CrawlBase` status. For example, it might set the time for the _next_ fetch based on whether the document changed last time it was fetched, and so cause pages that aren't changing very often to be examined less frequently than others.

## dbupdate:outlinks

A web page or an API might refer to other pages from which more data can be retrieved. The `dbupdate:outlinks` command is used to extract these further links depending on settings such as how deep to crawl, whether to bring in links that point outside of the initial website, etc.

# Usage

In your gulpfile:

```javascript
const gulp = require('gulp');
const nodeNutch = require('node-nutch');
```

Then, if using the filesystem as a `CrawlBase`:

```javascript
nodeNutch.addTasks(gulp, undefined, customParser, customExtract, customProcess);
```

Alternatively, if using S3 as a `CrawlBase`:

```javascript
const s3 = require('vinyl-s3');

s3.exists = function(glob, cb){
  var exists = false;

  this.src(glob)
    .on('data', function(){
      exists = true;
    })
    .on('end', function(){
      cb(exists);
    });
};

nodeNutch.addTasks(gulp, s3, customParser, customExtract, customProcess);
```
