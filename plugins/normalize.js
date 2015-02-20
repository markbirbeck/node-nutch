var config = require('../config/config');

var normalize = function (urlString){
  return config.urlnormalizer.order.reduce(function (value, normalizer){
    return normalizer.normalize(value);
  }, urlString);
};

module.exports = normalize;