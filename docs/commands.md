# Commands

E.g.:

    gulp inject

## clean:CrawlBase

Completely deletes the crawl database.

## inject

Inject some URLs into the crawl database, ready for crawling. The list of URLs to inject will come from files in the seeds directory. All files are read and each line in the file is processed, although any line that begins with a '#' is ignored.