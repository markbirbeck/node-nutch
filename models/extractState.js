/**
 * Class to handle extract state:
 */

var ExtractState = function (state){
  this.state = state || ExtractState.NOTEXTRACTED;
};

ExtractState.NOTEXTRACTED = 'notextracted';
ExtractState.SUCCESS = 'success';
ExtractState.FAILED = 'failed';

module.exports = ExtractState;