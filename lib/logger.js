/**
 * Created by yang on 2015/8/25.
 */
'use strict';

var _logger = console;

var logger = {
    setLogger: function(newLogger) {
        _logger = newLogger;
    }
};

var methods = 'log trace debug info warn error fatal'.split(' ');
methods.forEach(function(method) {
    logger[method] = function() {
        var oriLog = _logger[method] || _logger['log'];
        oriLog.apply(_logger, arguments);
    };
});

module.exports = logger;
