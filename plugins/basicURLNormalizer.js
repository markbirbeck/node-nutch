var URI = require('URIjs');

var BasicURLNormalizer = function (){};

BasicURLNormalizer.prototype.normalize = function(urlString){
  return URI(urlString)
    .fragment('')
    .normalize()
    .toString();
};

module.exports = new BasicURLNormalizer();