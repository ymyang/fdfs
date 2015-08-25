/**
 * Author: chenboxiang
 * Date: 14-6-15
 * Time: 下午1:59
 */
'use strict';

var _logger = console

var logger = {
    setLogger: function(newLogger) {
        _logger = newLogger
    }
}

var methods = 'log trace debug info warn error fatal'.split(' ')
methods.forEach(function(method) {
    logger[method] = function() {
        var oriLog = _logger[method] || _logger['log']
        oriLog.apply(_logger, arguments)
    }
})

module.exports = logger

