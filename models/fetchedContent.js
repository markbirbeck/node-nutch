/**
 * Class to handle fetched content:
 */

var FetchedContent = function(status, headers, content){
  this.status = status;
  this.headers = headers;
  this.content = content;
};

module.exports = FetchedContent;