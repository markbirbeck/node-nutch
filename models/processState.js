/**
 * Class to handle process state:
 */

var ProcessState = function (state){
  this.state = state || ProcessState.NOTPROCESSED;
};

ProcessState.NOTPROCESSED = 'notprocessed';
ProcessState.SUCCESS = 'success';
ProcessState.FAILED = 'failed';

module.exports = ProcessState;
