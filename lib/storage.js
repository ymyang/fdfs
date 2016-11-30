/**
 * Created by yang on 2015/8/25.
 */
'use strict';

var fs = require('fs');
var net = require('net');
var is = require('is-type-of');
var Promise = require('bluebird');
var logger = require('./logger.js');
var protocol = require('./protocol.js');
var helpers = require('./helpers.js');

module.exports = Storage;

function Storage(config) {
    this.config = config;
    this._name = config.host + ':' + config.port;
}

// private methods

Storage.prototype._getConnection = function() {
    return this._newConnection();
};

Storage.prototype._newConnection = function() {
    var _self = this;
    return new Promise(function(resolve, reject) {
        var socket = new net.Socket();
        logger.debug('connect to storage server [%s].', _self._name);
        socket.setTimeout(_self.config.timeout);
        socket.connect(_self.config.port, _self.config.host);

        socket.on('error', function(err) {
            reject(err);
        });

        socket.on('timeout', function() {
            socket.destroy();
            reject(new Error('connect to storage server [' + _self._name + '] timeout.'));
        });

        socket.on('connect', function() {
            logger.debug('storage server [%s] is connected', _self._name);
            resolve(socket);
        });
    });

};

/**
 * # request body:
 @ 1 byte: store path index on the storage server
 @ FDFS_PROTO_PKG_LEN_SIZE bytes: file size
 @ FDFS_FILE_EXT_NAME_MAX_LEN bytes: file ext name, do not include dot (.)
 @ file size bytes: file content

 # response body:
 @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
 @ filename bytes: filename
 * @param command
 * @param file
 * @param options
 */
Storage.prototype._uploadFile = function(command, file, options) {
    var _self = this;
    return this._getConnection().then(function(socket) {
        logger.debug('start upload file to storage server [%s]', _self._name);
        // 封装header并发送
        var bodyLength = 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE + protocol.FDFS_FILE_EXT_NAME_MAX_LEN + options.size;
        var header = protocol.packHeader(command, bodyLength, 0);
        socket.write(header);

        // 封装并发送body
        // 除file content外的内容
        var buffer = new Buffer(1 + protocol.FDFS_PROTO_PKG_LEN_SIZE + protocol.FDFS_FILE_EXT_NAME_MAX_LEN);
        buffer.fill(0);
        buffer.writeUInt8(_self.config.storePathIndex, 0);
        helpers.number2Buffer(options.size, protocol.FDFS_PROTO_PKG_LEN_SIZE).copy(buffer, 1);
        var extBL = Buffer.byteLength(options.ext, _self.config.charset);
        buffer.write(options.ext, 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE, extBL, _self.config.charset);

        socket.write(buffer);

        // 发送file content
        if (is.string(file)) {
            file = fs.createReadStream(file);
        }

        // buffer
        if (is.buffer(file)) {
            socket.write(file);

            // stream
        } else {
            file.pipe(socket, {end: false});
        }

        return new Promise(function(resolve, reject) {
            protocol.recvPacket(
                socket,
                protocol.STORAGE_PROTO_CMD_RESP,
                null,
                function(err, body) {
                    if (err) {
                        reject(err);
                        return;
                    }
                    // 校验body
                    if (body.length <= protocol.FDFS_GROUP_NAME_MAX_LEN) {
                        throw new Error('response body length: ' + body.length + ' <= ' + protocol.FDFS_GROUP_NAME_MAX_LEN);
                    }

                    var fileId = _parseFileId(body, _self.config.charset);
                    resolve(fileId);
                }
            );
        });
    });
};

/**
 * # request body:
 @ FDFS_PROTO_PKG_LEN_SIZE bytes: filename length
 @ FDFS_PROTO_PKG_LEN_SIZE bytes: file size
 @ filename bytes: filename
 @ file size bytes: file content

 # response body: none
 * @param file
 * @param options
 */
Storage.prototype._appendFile = function(file, options) {
    var _self = this;
    return this._getConnection().then(function(socket) {
        logger.debug('start upload file to storage server [%s]', _self._name);
        var gf = helpers.id2gf(options.fileId);
        // 封装header并发送
        var filenameBL = Buffer.byteLength(gf.filename, _self.config.charset);
        var bodyLength = 2*protocol.FDFS_PROTO_PKG_LEN_SIZE + filenameBL + options.size;
        var command = protocol.STORAGE_PROTO_CMD_APPEND_FILE;
        var header = protocol.packHeader(command, bodyLength, 0) ;
        socket.write(header);

        // 封装并发送body
        // 除file content外的内容
        var buffer = new Buffer(2*protocol.FDFS_PROTO_PKG_LEN_SIZE + filenameBL);
        buffer.fill(0);
        helpers.number2Buffer(gf.filename.length, protocol.FDFS_PROTO_PKG_LEN_SIZE).copy(buffer, 0);
        helpers.number2Buffer(options.size, protocol.FDFS_PROTO_PKG_LEN_SIZE).copy(buffer, protocol.FDFS_PROTO_PKG_LEN_SIZE);
        buffer.write(gf.filename, 2*protocol.FDFS_PROTO_PKG_LEN_SIZE, filenameBL, _self.config.charset);

        socket.write(buffer);

        // 发送file content
        if (is.string(file)) {
            file = fs.createReadStream(file);
        }

        // buffer
        if (is.buffer(file)) {
            socket.write(file);

            // stream
        } else {
            file.pipe(socket, {end: false});
        }

        return new Promise(function(resolve, reject) {
            protocol.recvPacket(
                socket,
                protocol.STORAGE_PROTO_CMD_RESP,
                0,
                function(err) {
                    if (err) {
                        reject(err);
                        return;
                    }
                    resolve();
                }
            );
        });
    });

};

/**
 * # request body:
 @ FDFS_PROTO_PKG_LEN_SIZE bytes: filename length
 @ FDFS_PROTO_PKG_LEN_SIZE bytes: file offset
 @ FDFS_PROTO_PKG_LEN_SIZE bytes: file size
 @ filename bytes: filename
 @ file size bytes: file content

 # response body: none

 * @param file
 * @param options
 */
Storage.prototype._modifyFile = function(file, options) {
    var _self = this;
    return this._getConnection().then(function(socket) {
        logger.debug('start upload file to storage server [%s]', _self._name);
        var gf = helpers.id2gf(options.fileId);
        // 封装header并发送
        var filenameBL = Buffer.byteLength(gf.filename, _self.config.charset);
        var bodyLength = 3*protocol.FDFS_PROTO_PKG_LEN_SIZE + filenameBL + options.size;
        var command = protocol.STORAGE_PROTO_CMD_MODIFY_FILE;
        var header = protocol.packHeader(command, bodyLength, 0);
        socket.write(header);

        // 封装并发送body
        // 除file content外的内容
        var buffer = new Buffer(3*protocol.FDFS_PROTO_PKG_LEN_SIZE + filenameBL);
        buffer.fill(0);
        helpers.number2Buffer(gf.filename.length, protocol.FDFS_PROTO_PKG_LEN_SIZE).copy(buffer, 0);
        helpers.number2Buffer(options.offset, protocol.FDFS_PROTO_PKG_LEN_SIZE).copy(buffer, protocol.FDFS_PROTO_PKG_LEN_SIZE);
        helpers.number2Buffer(options.size, protocol.FDFS_PROTO_PKG_LEN_SIZE).copy(buffer, 2*protocol.FDFS_PROTO_PKG_LEN_SIZE);
        buffer.write(gf.filename, 3*protocol.FDFS_PROTO_PKG_LEN_SIZE, filenameBL, _self.config.charset);

        socket.write(buffer);

        // 发送file content
        if (is.string(file)) {
            file = fs.createReadStream(file);
        }

        // buffer
        if (is.buffer(file)) {
            socket.write(file);

            // stream
        } else {
            file.pipe(socket, {end: false});
        }

        return new Promise(function(resolve, reject) {
            protocol.recvPacket(
                socket,
                protocol.STORAGE_PROTO_CMD_RESP,
                0,
                function(err) {
                    if (err) {
                        reject(err);
                        return;
                    }
                    resolve();
                }
            );
        });
    });

};

// upload相关

// public methods

/**
 * # request body:
     @ 1 byte: store path index on the storage server
     @ FDFS_PROTO_PKG_LEN_SIZE bytes: file size
     @ FDFS_FILE_EXT_NAME_MAX_LEN bytes: file ext name, do not include dot (.)
     @ file size bytes: file content

   # response body:
     @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
     @ filename bytes: filename
 * @param file
 * @param options
 */
Storage.prototype.upload = function(file, options) {
    if (!options.method || options.method === protocol.FDFS_METHOD_UPLOAD_FILE) {
        var command = protocol.STORAGE_PROTO_CMD_UPLOAD_FILE;
        return this._uploadFile(command, file, options);
    } else if (options.method === protocol.FDFS_METHOD_UPLOAD_APPENDER_FILE) {
        var command = protocol.STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE;
        return this._uploadFile(command, file, options);
    } else if (options.method === protocol.FDFS_METHOD_APPEND_FILE) {
        return this._appendFile(file, options);
    } else if (options.method === protocol.FDFS_METHOD_MODIFY_FILE) {
        return this._modifyFile(file, options);
    }
    return Promise.reject(new Error("options.method must in ['upload', 'uploadAppender', 'append', 'modify'] "));
};

/**
 * * STORAGE_PROTO_CMD_SET_METADATA
 *
     # function: set meta data
     # request body:
         @ FDFS_PROTO_PKG_LEN_SIZE bytes: filename length
         @ FDFS_PROTO_PKG_LEN_SIZE bytes: meta data size
         @ 1 bytes: operation flag,
             'O' for overwrite all old metadata
             'M' for merge, insert when the meta item not exist, otherwise update it
         @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
         @ filename bytes: filename
         @ meta data bytes: each meta data seperated by \x01,
         name and value seperated by \x02
     # response body: none
 * @param fileId
 * @param metaData
 */
Storage.prototype.setMetaData = function(fileId, metaData, flag) {
    flag = flag || protocol.STORAGE_SET_METADATA_FLAG_OVERWRITE;

    var _self = this;
    return this._getConnection().then(function(socket) {
        var gf = helpers.id2gf(fileId);
        var packedMeta = protocol.packMetaData(metaData);

        // 封装header
        var charset = _self.config.charset;
        var command = protocol.STORAGE_PROTO_CMD_SET_METADATA;
        var fnLength = Buffer.byteLength(gf.filename, charset);
        var metaLength = Buffer.byteLength(packedMeta, charset);
        var bodyLength = protocol.FDFS_PROTO_PKG_LEN_SIZE + protocol.FDFS_PROTO_PKG_LEN_SIZE + 1 +
            protocol.FDFS_GROUP_NAME_MAX_LEN + fnLength + metaLength;

        var header = protocol.packHeader(command, bodyLength, 0);
        socket.write(header);

        // 封装body
        var groupLength = Buffer.byteLength(gf.group, charset);
        var body = new Buffer(bodyLength);
        body.fill(0);
        helpers.number2Buffer(fnLength, protocol.FDFS_PROTO_PKG_LEN_SIZE).copy(body, 0);
        helpers.number2Buffer(metaLength, protocol.FDFS_PROTO_PKG_LEN_SIZE).copy(body, protocol.FDFS_PROTO_PKG_LEN_SIZE);
        body.write(flag, protocol.FDFS_PROTO_PKG_LEN_SIZE + protocol.FDFS_PROTO_PKG_LEN_SIZE, 1, charset);
        body.write(gf.group, protocol.FDFS_PROTO_PKG_LEN_SIZE + protocol.FDFS_PROTO_PKG_LEN_SIZE + 1, groupLength, charset);
        body.write(gf.filename, protocol.FDFS_PROTO_PKG_LEN_SIZE + protocol.FDFS_PROTO_PKG_LEN_SIZE + 1 + protocol.FDFS_GROUP_NAME_MAX_LEN, fnLength, charset);
        body.write(packedMeta, bodyLength - metaLength, metaLength, charset);

        socket.write(body);

        return new Promise(function(resolve, reject) {
            protocol.recvPacket(
                socket,
                protocol.STORAGE_PROTO_CMD_RESP,
                0,
                function(err) {
                    if (err) {
                        reject(err);
                        return;
                    }
                    resolve();
                }
            );
        });
    });

};

/**
 * * STORAGE_PROTO_CMD_GET_METADATA
     # function: get metat data from storage server
     # request body:
     @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
     @ filename bytes: filename
     # response body
     @ meta data buff, each meta data seperated by \x01, name and value seperated by \x02
 * @param fileId
 */
Storage.prototype.getMetaData = function(fileId) {
    var _self = this;
    return this._getConnection().then(function(socket) {
        var gf = helpers.id2gf(fileId);
        var packet = protocol.packFileId(protocol.STORAGE_PROTO_CMD_GET_METADATA, gf.group, gf.filename, _self.config.charset);
        socket.write(packet);

        return new Promise(function(resolve, reject) {
            protocol.recvPacket(
                socket,
                protocol.STORAGE_PROTO_CMD_RESP,
                null,
                function(err, body) {
                    if (err) {
                        reject(err);
                        return;
                    }
                    var rawMeta = body.toString(_self.config.charset);
                    if (rawMeta) {
                        var metaData = protocol.parseMetaData(rawMeta);
                        resolve(metaData);
                    } else {
                        resolve(rawMeta);
                    }
                }
            );
        });
    });
};

/**
 * 删除文件
 * STORAGE_PROTO_CMD_DELETE_FILE
 * # request body:
     @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
     @ filename bytes: filename

   # response body: none
 * @param fileId
 */
Storage.prototype.del = function(fileId) {
    var _self = this;
    return this._getConnection().then(function(socket) {
        var gf = helpers.id2gf(fileId);
        var packet = protocol.packFileId(protocol.STORAGE_PROTO_CMD_DELETE_FILE, gf.group, gf.filename, _self.config.charset);
        socket.write(packet);

        return new Promise(function(resolve, reject) {
            protocol.recvPacket(
                socket,
                protocol.STORAGE_PROTO_CMD_RESP,
                0,
                function(err) {
                    if (err) {
                        reject(err);
                        return;
                    }
                    resolve();
                }
            );
        });
    });
};

/**
 * STORAGE_PROTO_CMD_DOWNLOAD_FILE
     # function: download/fetch file from storage server
     # request body:
         @ FDFS_PROTO_PKG_LEN_SIZE bytes: file offset
         @ FDFS_PROTO_PKG_LEN_SIZE bytes: download file bytes
         @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
         @ filename bytes: filename
 
     # response body:
        @ file content
 * @param fileId
 * @param options
 *      options.target 下载的文件流将被写入到这里，可以是本地文件名，也可以是WritableStream
 *      options.offset和options.bytes: 当只想下载文件中的某1片段时指定
 */
Storage.prototype.download = function(fileId, options) {
    var _self = this;
    return this._getConnection().then(function(socket) {
        var gf = helpers.id2gf(fileId);
        var charset = _self.config.charset;
        // 封装header
        var fnLength = Buffer.byteLength(gf.filename, charset);
        var bodyLength = protocol.FDFS_PROTO_PKG_LEN_SIZE + protocol.FDFS_PROTO_PKG_LEN_SIZE + protocol.FDFS_GROUP_NAME_MAX_LEN + fnLength;
        var header = protocol.packHeader(protocol.STORAGE_PROTO_CMD_DOWNLOAD_FILE, bodyLength, 0);

        // 封装body
        var body = new Buffer(bodyLength);
        // 默认都填充上0
        body.fill(0);
        if (options.offset) {
            helpers.number2Buffer(options.offset, protocol.FDFS_PROTO_PKG_LEN_SIZE).copy(body);
        }
        if (options.bytes) {
            helpers.number2Buffer(options.bytes, protocol.FDFS_PROTO_PKG_LEN_SIZE).copy(body, protocol.FDFS_PROTO_PKG_LEN_SIZE);
        }
        var groupBL = Buffer.byteLength(gf.group, charset);
        body.write(gf.group, protocol.FDFS_PROTO_PKG_LEN_SIZE + protocol.FDFS_PROTO_PKG_LEN_SIZE, groupBL, charset);
        body.write(gf.filename, protocol.FDFS_PROTO_PKG_LEN_SIZE + protocol.FDFS_PROTO_PKG_LEN_SIZE + protocol.FDFS_GROUP_NAME_MAX_LEN, fnLength, charset);

        socket.write(Buffer.concat([header, body]));

        return new Promise(function(resolve, reject) {
            var header;
            var target = options.target;
            // 已接收的body length
            var recvLength = 0;

            protocol.recvPacket(
                socket,
                protocol.STORAGE_PROTO_CMD_RESP,
                null,
                function(err, data) {
                    if (err) {
                        reject(err);
                        return;
                    }
                    // header
                    if (!is.buffer(data)) {
                        header = data;

                        // body
                    } else {
                        if (!target) {
                            resolve({
                                data: data,
                                bodyLength: header.bodyLength
                            });
                        } else {
                            target.write(data);
                        }

                        recvLength += data.length;
                        // 读取完毕
                        if (recvLength >= header.bodyLength) {
                            // 读取完毕要关闭文件。
                            // 不关闭文件造成两种后果：1、文件句柄不释放，造成硬盘空间不释放。
                            // 2、在文件还没有写完时候就返回文件,表现如express中返回图片只有一半。
                            target.end();
                            target.on('close', function () {
                              protocol.closeSocket(socket);
                              resolve();
                            });
                        }
                    }
                },
                true
            );
        });
    });
};

/**
 * * STORAGE_PROTO_CMD_QUERY_FILE_INFO
     # function: query file info from storage server
     # request body:
        @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
        @ filename bytes: filename

     # response body:
        @ FDFS_PROTO_PKG_LEN_SIZE bytes: file size
        @ FDFS_PROTO_PKG_LEN_SIZE bytes: file create timestamp
        @ FDFS_PROTO_PKG_LEN_SIZE bytes: file CRC32 signature
        @ FDFS_IPADDR_SIZE bytes: file source ip addr
 * @param fileId
 */
Storage.prototype.getFileInfo = function(fileId) {
    var charset = this.config.charset;
    return this._getConnection().then(function(socket) {
        var gf = helpers.id2gf(fileId);
        var packet = protocol.packFileId(protocol.STORAGE_PROTO_CMD_QUERY_FILE_INFO, gf.group, gf.filename, charset);
        socket.write(packet);

        return new Promise(function(resolve, reject) {
            protocol.recvPacket(
                socket,
                protocol.STORAGE_PROTO_CMD_RESP,
                protocol.FDFS_PROTO_PKG_LEN_SIZE * 3 + protocol.FDFS_IPADDR_SIZE,
                function(err, body) {
                    if (err) {
                        reject(err);
                        return;
                    }
                    var result = {
                        size: parseInt(body.toString('hex', 0, protocol.FDFS_PROTO_PKG_LEN_SIZE), 16),
                        timestamp: helpers.buffer2Number(body, protocol.FDFS_PROTO_PKG_LEN_SIZE),
                        crc32: helpers.buffer2Number(body, protocol.FDFS_PROTO_PKG_LEN_SIZE * 2),
                        addr: helpers.trim(body.toString(charset, protocol.FDFS_PROTO_PKG_LEN_SIZE * 3))
                    };
                    resolve(result);
                }
            );
        });
    });
};

// helpers
/**
 * parse file id from body
 * @param body
 * @param charset
 */
function _parseFileId(body, charset) {
    var group = helpers.trim(body.toString(charset, 0, protocol.FDFS_GROUP_NAME_MAX_LEN));
    var filename = helpers.trim(body.toString(charset, protocol.FDFS_GROUP_NAME_MAX_LEN));
    return group + '/' + filename;
}
