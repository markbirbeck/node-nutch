/**
 * Class to handle parse state:
 */

var ParseState = function (state){
  this.state = state || ParseState.NOTPARSED;
};

ParseState.NOTPARSED = 'notparsed';
ParseState.SUCCESS = 'success';
ParseState.FAILED = 'failed';

module.exports = ParseState;