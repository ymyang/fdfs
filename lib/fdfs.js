/**
 * Created by yang on 2015/8/25.
 */
'use strict';

var path = require('path');
var fs = require('fs');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var _ = require('lodash');
var is = require('is-type-of');
var Tracker = require('./tracker.js');
var logger = require('./logger.js');
var helpers = require('./helpers.js');
var protocol = require('./protocol.js');


var defaults = {
    charset: 'utf8',
    trackers: [],
    // 默认超时时间10s
    timeout: 10000,
    // 默认后缀
    // 当获取不到文件后缀时使用
    defaultExt: ''
};

function FdfsClient(config) {
    EventEmitter.call(this);
    // config global logger
    if (config && config.logger) {
        logger.setLogger(config.logger);
    }
    this.config = _.extend({}, defaults, config);

    this._checkConfig();
    this._init();
    this._errorHandle();
}

// extends from EventEmitter
util.inherits(FdfsClient, EventEmitter);

// ------------- private methods
/**
 * 确认配置是否合法
 * @private
 */
FdfsClient.prototype._checkConfig = function() {

    // ------------- 验证trackers是否合法
    if (!this.config.trackers) {
        throw new Error('you must specify "trackers" in config.');
    }

    if (!Array.isArray(this.config.trackers)) {
        this.config.trackers = [this.config.trackers];
    }

    if (this.config.trackers.length === 0) {
        throw new Error('"trackers" in config is empty.');
    }

    this.config.trackers.forEach(function(tracker) {
        if (!tracker.host || !tracker.port) {
            throw new Error('"trackers" in config is invalid, every tracker must all have "host" and "port".');
        }
    });
};

FdfsClient.prototype._init = function() {
    // --------- init trackers
    var self = this;
    this._trackers = [];
    this.config.trackers.forEach(function(tc) {
        tc.timeout = self.config.timeout;
        tc.charset = self.config.charset;
        var tracker = new Tracker(tc) ;
        self._trackers.push(tracker);
        tracker.on('error', function(err) {
            logger.error(err);
            // 将有错误的tracker剔除
            self._trackers.splice(self._trackers.indexOf(tracker), 1);
            // 检查是否还有可用的tracker
            if (self._trackers.length === 0) {
                self.emit('error', new Error('There are no available trackers, please check your tracker config or your tracker server.'));
            }
        });
    });
};

FdfsClient.prototype._errorHandle = function() {
    // 1. 当没有tracker可用时触发
    // 2. 当连接storage错误时触发
    this.on('error', function(err) {
        logger.error(err);
    });
};

/**
 * 按顺序获取可用的tracker
 * @private
 */
FdfsClient.prototype._getTracker = function() {
    if (!this._trackerIndex) {
        this._trackerIndex = 0;
        return this._trackers[0];

    } else {
        this._trackerIndex++;
        if (this._trackerIndex >= this._trackers.length) {
            this._trackerIndex = 0;
        }
        return this._trackers[this._trackerIndex];
    }
};

FdfsClient.prototype._upload = function(file, options, callback) {
    var tracker = this._getTracker();

    tracker.getStoreStorage(options.group, function(err, storage) {
        if (err) {
            callback(err);
            return;
        }
        storage.upload(file, options, callback);
    })
};

// ------------- public methods

/**
 * 上传文件
 * @param file absolute file path or Buffer or ReadableStream
 * @param options
 *      options.group: 指定要上传的group, 不指定则由tracker server分配
 *      options.size: file size, file参数为ReadableStream时必须指定
 *      options.ext: 上传文件的后缀，不指定则获取file参数的后缀，不含(.)
 * @param callback
 */
FdfsClient.prototype.upload = function(file, options, callback) {
    var self = this;
    if (is.function(options)) {
        callback = options;
        options = {};

    } else {
        if (!options) {
            options = {};
        }
    }

    _normalizeUploadParams(file, options, function(err) {
        if (err) {
            callback(err);
            return;
        }
        if (!options.ext) {
            options.ext = self.config.defaultExt;
        }

        if (!options.group && options.fileId) {
            var gf = helpers.id2gf(options.fileId);
            options.group = gf.group;
        }

        self._upload(file, options, callback);
    });
};

/**
 * 下载文件
 * @param fileId
 * @param options options可以直接传options.target
 *      options.target 下载的文件流将被写入到这里，可以是本地文件名，也可以是WritableStream，如果为空则每次服务器返回数据的时候都会回调callback
 *      options.offset和options.bytes: 当只想下载文件中的某1片段时指定
 * @param callback 若未指定options.target，服务器每次数据的返回都会回调，若指定了options.target，则只在结束时回调一次
 */
FdfsClient.prototype.download = function(fileId, options, callback) {
    if (!options || is.function(options)) {
        callback(new Error('options.target is not specified'));
        return;
    }

    // 直接传入target
    if (!options.target) {
        var ori = options;
        options = {};
        options.target = ori;
    }

    if (!(is.string(options.target) || is.writableStream(options.target))) {
        callback(new Error('options.target is invalid, it\'s type must be String or WritableStream'));
    }

    if (is.string(options.target)) {
        options.target = fs.createWriteStream(options.target);
    }

    this._getTracker().getFetchStorage(fileId, function(err, storage) {
        storage.download(fileId, options, callback);
    });
};

/**
 * 删除fileId指定的文件
 * @param fileId
 * @param callback
 */
FdfsClient.prototype.del = function(fileId, callback) {
    this._getTracker().getUpdateStorage(fileId, function(err, storage) {
        if (err) {
            callback(err);
            return;
        }
        storage.del(fileId, callback);
    });
};
FdfsClient.prototype.remove = FdfsClient.prototype.del;

/**
 * @param fileId
 * @param metaData  {key1: value1, key2: value2}
 * @param flag 'O' for overwrite all old metadata (default)
 'M' for merge, insert when the meta item not exist, otherwise update it
 * @param callback
 */
FdfsClient.prototype.setMetaData = function(fileId, metaData, flag, callback) {
    if (is.function(flag)) {
        callback = flag;
        flag = 'O';
    }

    this._getTracker().getUpdateStorage(fileId, function(err, storage) {
        if (err) {
            callback(err);
            return;
        }
        storage.setMetaData(fileId, metaData, flag, callback);
    });
};

/**
 * 获取指定fileId的meta data
 * @param fileId
 * @param callback
 */
FdfsClient.prototype.getMetaData = function(fileId, callback) {
    this._getTracker().getUpdateStorage(fileId, function(err, storage) {
        if (err) {
            callback(err);
            return;
        }
        storage.getMetaData(fileId, callback);
    });
};

/**
 * 获取指定fileId的信息
 * fileInfo会传给回调，结构如下
 *  {
 *      // 文件大小
 *      size:
 *      // 文件创建的UTC时间戳，单位为秒
 *      timestamp:
 *      crc32:
 *      // 最初上传到的storage server的ip
 *      addr:
 *  }
 * @param fileId
 * @param callback
 */
FdfsClient.prototype.getFileInfo = function(fileId, callback) {
    this._getTracker().getUpdateStorage(fileId, function(err, storage) {
        if (err) {
            callback(err);
            return;
        }
        storage.getFileInfo(fileId, callback);
    });
};

// -------------- helpers
/**
 * 验证file参数是否合法，同时补充一些必要的参数
 * 若为String，则需验证是否存在
 * 若为ReadableStream，则需验证options.size是否存在
 * @param file
 * @param options
 * @param callback
 * @private
 */
function _normalizeUploadParams(file, options, callback) {
    if (!file) {
        callback(new Error('The "file" parameter is empty.'));
        return;
    }

    if (!(is.string(file) || is.buffer(file) || is.readableStream(file))) {
        callback(new Error('The "file" parameter is invalid, it must be a String, Buffer, or ReadableStream'));
        return;
    }

    if (is.string(file)) {
        fs.stat(file, function(err, stats) {
            if (err || !stats) {
                callback(new Error('File [' + file + '] is not exists!'));
                return;
            }

            options.size = stats.size;
            if (!options.ext) {
                options.ext = path.extname(file);
                if (options.ext) {
                    // 去掉.
                    options.ext = options.ext.substring(1);
                }
            }
            callback();
        });
        return;
    }

    if (is.readableStream(file) && !options.size) {
        callback(new Error('when the "file" parameter\'s is ReadableStream, options.size must specified'));
        return;
    }

    if (is.buffer(file)) {
        options.size = file.length;
    }

    // TODO
    if (options.method === protocol.FDFS_METHOD_UPLOAD_APPENDER_FILE) {

    } else if (options.method === protocol.FDFS_METHOD_APPEND_FILE) {
        if (!options.fileId) {
            callback(new Error('options.fileId is missed'));
        }
    } else if (options.method === protocol.FDFS_METHOD_MODIFY_FILE) {
        if (!options.fileId) {
            callback(new Error('options.fileId is missed'));
        }
        if (!(options.offset || options.offset === 0)) {
            callback(new Error('options.offset is missed'));
        }
        if (!is.number(options.offset)) {
            callback(new Error('options.offset must be a number'));
        }
    }

    callback();
}


//exports
module.exports = FdfsClient;