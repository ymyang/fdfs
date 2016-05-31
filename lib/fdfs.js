/**
 * Created by yang on 2015/8/25.
 */
'use strict';

var path = require('path');
var fs = require('fs');
var net = require('net');
var _ = require('lodash');
var is = require('is-type-of');
var Promise = require('bluebird');
var Tracker = require('./tracker.js');
var logger = require('./logger.js');
var helpers = require('./helpers.js');
var protocol = require('./protocol.js');


// exports
module.exports = FdfsClient;

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
    // config global logger
    if (config && config.logger) {
        logger.setLogger(config.logger);
    }
    this.config = _.extend({}, defaults, config);

    this._checkConfig();
}

// private methods
/**
 * 确认配置是否合法
 * @private
 */
FdfsClient.prototype._checkConfig = function() {

    // 验证trackers是否合法
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

/**
 * 按顺序获取可用的tracker
 */
FdfsClient.prototype._getTracker = function() {
    if (!this._trackerIndex) {
        this._trackerIndex = 0;
    } else {
        this._trackerIndex++;
        if (this._trackerIndex >= this.config.trackers.length) {
            this._trackerIndex = 0;
        }
    }
    var _self = this;
    var _tracker = undefined;
    var config = this._getTrackerConfig(this._trackerIndex);
    return _getTrackerConnection(config).then(function(tracker) {
        if (tracker) {
            _tracker = tracker;
            return tracker;
        }
        return Promise.reduce(_.range(_self.config.trackers.length), function(total, index) {
            if (_tracker) {
                return _tracker;
            }
            if (index == _self._trackerIndex) {
                return;
            }
            config = _self._getTrackerConfig(index);
            return _getTrackerConnection(config).then(function(tr) {
                _tracker = tr;
            });
        }, 0);

    }).then(function() {
        if (_tracker) {
            return _tracker;
        }
        throw new Error('all trackers connect fail, please check your tracker config or your tracker server.');
    });
};

FdfsClient.prototype._getTrackerConfig = function(index) {
    var config = this.config.trackers[index];
    config.timeout = this.config.timeout;
    config.charset = this.config.charset;
    return config;
};


function _getTrackerConnection(config) {
    return new Promise(function(resolve, reject) {
        var _name = config.host + ':' + config.port;
        var socket = new net.Socket();
        logger.debug('connect to tracker server [%s]', _name);
        socket.setTimeout(config.timeout);
        socket.connect(config.port, config.host);

        socket.on('error', function(err) {
            logger.error('connect to tracker server [' + _name + '] err:', err);
            resolve();
        });

        socket.on('timeout', function() {
            socket.destroy();
            logger.error('connect to tracker server [' + _name + '] timeout.');
            resolve();
        });

        socket.on('connect', function() {
            logger.debug('tracker server [%s] is connected', _name);
            resolve(new Tracker(config, socket));
        });
    });
}

// public methods

/**
 * 上传文件
 * @param file absolute file path or Buffer or ReadableStream
 * @param options
 *      options.group: 指定要上传的group, 不指定则由tracker server分配
 *      options.size: file size, file参数为ReadableStream时必须指定
 *      options.ext: 上传文件的后缀，不指定则获取file参数的后缀，不含(.)
 */
FdfsClient.prototype.upload = function(file, options) {
    var _self = this;
    options = options || {};
    return _normalizeUploadParams(file, options).then(function() {
        if (!options.ext) {
            options.ext = _self.config.defaultExt;
        }

        if (!options.group && options.fileId) {
            var gf = helpers.id2gf(options.fileId);
            options.group = gf.group;
        }
        return _self._getTracker();
    }).then(function(tracker) {
        return tracker.getStoreStorage(options.group);
    }).then(function(storage) {
        return storage.upload(file, options);
    });
};

/**
 * 下载文件
 * @param fileId
 * @param options options可以直接传options.target
 *      options.target 下载的文件流将被写入到这里，可以是本地文件名，也可以是WritableStream
 *      options.offset和options.bytes: 当只想下载文件中的某1片段时指定
 */
FdfsClient.prototype.download = function(fileId, options) {
    var _self = this;
    return new Promise(function(resolve, reject) {
        if (!options || is.function(options)) {
            reject(new Error('options.target is not specified'));
            return;
        }

        // 直接传入target
        if (!options.target) {
            var ori = options;
            options = {};
            options.target = ori;
        }

        if (!(is.string(options.target) || is.writableStream(options.target))) {
            reject(new Error('options.target is invalid, it\'s type must be String or WritableStream'));
            return;
        }

        if (is.string(options.target)) {
            options.target = fs.createWriteStream(options.target);
        }
        resolve();
    }).then(function() {
        return _self._getTracker();
    }).then(function(tracker) {
        return tracker.getFetchStorage(fileId);
    }).then(function(storage) {
        return storage.download(fileId, options);
    });
};

/**
 * 删除fileId指定的文件
 * @param fileId
 */
FdfsClient.prototype.del = function(fileId) {
    return this._getTracker().then(function(tracker) {
        return tracker.getUpdateStorage(fileId);
    }).then(function(storage) {
        return storage.del(fileId);
    });
};

FdfsClient.prototype.remove = FdfsClient.prototype.del;

/**
 * @param fileId
 * @param metaData  {key1: value1, key2: value2}
 * @param flag 'O' for overwrite all old metadata (default)
 'M' for merge, insert when the meta item not exist, otherwise update it
 */
FdfsClient.prototype.setMetaData = function(fileId, metaData, flag) {
    return this._getTracker().then(function(tracker) {
        return tracker.getUpdateStorage(fileId);
    }).then(function(storage) {
        return storage.setMetaData(fileId, metaData, flag);
    });
};

/**
 * 获取指定fileId的meta data
 * @param fileId
 */
FdfsClient.prototype.getMetaData = function(fileId) {
    return this._getTracker().then(function(tracker) {
        return tracker.getUpdateStorage(fileId);
    }).then(function(storage) {
        return storage.getMetaData(fileId);
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
 */
FdfsClient.prototype.getFileInfo = function(fileId) {
    return this._getTracker().then(function(tracker) {
        return tracker.getUpdateStorage(fileId);
    }).then(function(storage) {
        return storage.getFileInfo(fileId);
    });
};

FdfsClient.prototype.listGroups = function() {
    return this._getTracker().then(function(tracker) {
        return tracker.listGroups();
    });
};

FdfsClient.prototype.listStorages = function(group) {
    return this._getTracker().then(function(tracker) {
        return tracker.listStorages(group);
    });
};

// helpers
/**
 * 验证file参数是否合法，同时补充一些必要的参数
 * 若为String，则需验证是否存在
 * 若为ReadableStream，则需验证options.size是否存在
 * @param file
 * @param options
 */
function _normalizeUploadParams(file, options) {
    return new Promise(function(resolve, reject) {
        if (!file) {
            reject(new Error('The "file" parameter is empty.'));
            return;
        }

        if (!(is.string(file) || is.buffer(file) || is.readableStream(file))) {
            reject(new Error('The "file" parameter is invalid, it must be a String, Buffer, or ReadableStream'));
            return;
        }


        if (is.readableStream(file) && !options.size) {
            reject(new Error('when the "file" parameter\'s is ReadableStream, options.size must specified'));
            return;
        }

        if (is.buffer(file)) {
            options.size = file.length;
        }

        if (is.string(options.size)) {
            options.size = Number(options.size);
        }

        // TODO
        if (options.method === protocol.FDFS_METHOD_UPLOAD_APPENDER_FILE) {

        } else if (options.method === protocol.FDFS_METHOD_APPEND_FILE) {
            if (!options.fileId) {
                reject(new Error('options.fileId is missed'));
            }
        } else if (options.method === protocol.FDFS_METHOD_MODIFY_FILE) {
            if (!options.fileId) {
                reject(new Error('options.fileId is missed'));
            }

            if (is.string(options.offset)) {
                options.offset = Number(options.offset);
            }

            if (!(options.offset || options.offset === 0)) {
                reject(new Error('options.offset is missed'));
            }
            if (!is.number(options.offset)) {
                reject(new Error('options.offset must be a number'));
            }
        }

        if (is.string(file)) {
            var stats = fs.statSync(file);
            options.size = stats.size;
            if (!options.ext) {
                options.ext = path.extname(file);
                if (options.ext) {
                    // 去掉.
                    options.ext = options.ext.substring(1);
                }
            }
            resolve();
        }
        resolve();
    });

}
