/**
 * Created by yang on 2015/8/25.
 */
'use strict';

var Buffer = require('buffer').Buffer;
var _ = require('lodash');
var is = require('is-type-of');
var helpers = require('./helpers.js');
var logger = require('./logger.js');

var protocol = {};

module.exports = protocol;

Object.defineProperties(protocol, {
    FDFS_PROTO_CMD_QUIT: helpers.buildConstProp(82),
    TRACKER_PROTO_CMD_SERVER_LIST_GROUP: helpers.buildConstProp(91),
    TRACKER_PROTO_CMD_SERVER_LIST_STORAGE: helpers.buildConstProp(92),
    TRACKER_PROTO_CMD_SERVER_DELETE_STORAGE: helpers.buildConstProp(93),

    TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE: helpers.buildConstProp(101),
    TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE: helpers.buildConstProp(102),
    TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE: helpers.buildConstProp(103),
    TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE: helpers.buildConstProp(104),
    TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ALL: helpers.buildConstProp(105),
    TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ALL: helpers.buildConstProp(106),
    TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ALL: helpers.buildConstProp(107),
    TRACKER_PROTO_CMD_RESP: helpers.buildConstProp(100),
    FDFS_PROTO_CMD_ACTIVE_TEST: helpers.buildConstProp(111),
    STORAGE_PROTO_CMD_UPLOAD_FILE: helpers.buildConstProp(11),
    STORAGE_PROTO_CMD_DELETE_FILE: helpers.buildConstProp(12),
    STORAGE_PROTO_CMD_SET_METADATA: helpers.buildConstProp(13),
    STORAGE_PROTO_CMD_DOWNLOAD_FILE: helpers.buildConstProp(14),
    STORAGE_PROTO_CMD_GET_METADATA: helpers.buildConstProp(15),
    STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE: helpers.buildConstProp(21),
    STORAGE_PROTO_CMD_QUERY_FILE_INFO: helpers.buildConstProp(22),
    STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE: helpers.buildConstProp(23),  //create appender file
    STORAGE_PROTO_CMD_APPEND_FILE: helpers.buildConstProp(24),  //append file
    STORAGE_PROTO_CMD_MODIFY_FILE: helpers.buildConstProp(34),  //modify appender file
    STORAGE_PROTO_CMD_TRUNCATE_FILE: helpers.buildConstProp(36),  //truncate appender file


    FDFS_STORAGE_STATUS_INIT: helpers.buildConstProp(0),
    FDFS_STORAGE_STATUS_WAIT_SYNC: helpers.buildConstProp(1),
    FDFS_STORAGE_STATUS_SYNCING: helpers.buildConstProp(2),
    FDFS_STORAGE_STATUS_IP_CHANGED: helpers.buildConstProp(3),
    FDFS_STORAGE_STATUS_DELETED: helpers.buildConstProp(4),
    FDFS_STORAGE_STATUS_OFFLINE: helpers.buildConstProp(5),
    FDFS_STORAGE_STATUS_ONLINE: helpers.buildConstProp(6),
    FDFS_STORAGE_STATUS_ACTIVE: helpers.buildConstProp(7),
    FDFS_STORAGE_STATUS_NONE: helpers.buildConstProp(99),

    /**
     * for overwrite all old metadata
     */
    STORAGE_SET_METADATA_FLAG_OVERWRITE: helpers.buildConstProp('O'),

    /**
     * for replace, insert when the meta item not exist, otherwise update it
     */
    STORAGE_SET_METADATA_FLAG_MERGE: helpers.buildConstProp('M'),

    FDFS_PROTO_PKG_LEN_SIZE: helpers.buildConstProp(8),
    FDFS_PROTO_CMD_SIZE: helpers.buildConstProp(1),
    FDFS_GROUP_NAME_MAX_LEN: helpers.buildConstProp(16),
    FDFS_IPADDR_SIZE: helpers.buildConstProp(16),
    FDFS_DOMAIN_NAME_MAX_SIZE: helpers.buildConstProp(128),
    FDFS_VERSION_SIZE: helpers.buildConstProp(6),
    FDFS_STORAGE_ID_MAX_SIZE: helpers.buildConstProp(16),

    FDFS_RECORD_SEPERATOR: helpers.buildConstProp('\u0001'),
    FDFS_FIELD_SEPERATOR: helpers.buildConstProp('\u0002'),

    FDFS_FILE_EXT_NAME_MAX_LEN: helpers.buildConstProp(6),
    FDFS_FILE_PREFIX_MAX_LEN: helpers.buildConstProp(16),
    FDFS_FILE_PATH_LEN: helpers.buildConstProp(10),
    FDFS_FILENAME_BASE64_LENGTH: helpers.buildConstProp(27),
    FDFS_TRUNK_FILE_INFO_LEN: helpers.buildConstProp(16),

    ERR_NO_ENOENT: helpers.buildConstProp(2),
    ERR_NO_EIO: helpers.buildConstProp(5),
    ERR_NO_EBUSY: helpers.buildConstProp(16),
    ERR_NO_EINVAL: helpers.buildConstProp(22),
    ERR_NO_ENOSPC: helpers.buildConstProp(28),
    ECONNREFUSED: helpers.buildConstProp(61),
    ERR_NO_EALREADY: helpers.buildConstProp(114),

    // 成功的STATUS
    HEADER_STATUS_SUCCESS: helpers.buildConstProp(0)
});

Object.defineProperties(protocol, {
    STORAGE_PROTO_CMD_RESP: helpers.buildConstProp(protocol.TRACKER_PROTO_CMD_RESP),

    TRACKER_QUERY_STORAGE_FETCH_BODY_LEN: helpers.buildConstProp(protocol.FDFS_GROUP_NAME_MAX_LEN + protocol.FDFS_IPADDR_SIZE - 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE),
    TRACKER_QUERY_STORAGE_STORE_BODY_LEN: helpers.buildConstProp(protocol.FDFS_GROUP_NAME_MAX_LEN + protocol.FDFS_IPADDR_SIZE + protocol.FDFS_PROTO_PKG_LEN_SIZE),

    PROTO_HEADER_CMD_INDEX: helpers.buildConstProp(protocol.FDFS_PROTO_PKG_LEN_SIZE),
    PROTO_HEADER_STATUS_INDEX: helpers.buildConstProp(protocol.FDFS_PROTO_PKG_LEN_SIZE + 1),

    HEADER_BYTE_LENGTH: helpers.buildConstProp(protocol.FDFS_PROTO_PKG_LEN_SIZE + 2)
});

// 协议相关的封装方法
_.extend(protocol, {

    FDFS_METHOD_UPLOAD_FILE: 'upload',
    FDFS_METHOD_UPLOAD_APPENDER_FILE: 'uploadAppender',
    FDFS_METHOD_APPEND_FILE: 'append',
    FDFS_METHOD_MODIFY_FILE: 'modify',

    /**
     * 封装协议头
     * @param command
     * @param bodyLength
     * @param status
     * @return {Buffer}
     */
    packHeader: function(command, bodyLength, status) {
        if (!bodyLength) {
            bodyLength = 0;
        }

        if (!status) {
            status = 0;
        }

        // ----------- 存放1字节的command和1字节的status
        var buffer = new Buffer(2);
        buffer.writeUInt8(command, 0);
        buffer.writeUInt8(status, 1);

        // 生成8bytes存放body length的buffer
        var blBuffer = helpers.number2Buffer(bodyLength, 8);

        // 拼接
        return Buffer.concat([blBuffer, buffer]);
    },

    /**
     * 解析返回的包
     * @param socket
     * @param expectedCommand
     * @param expectedBodyLength
     * @param callback
     * @param headerOnly 是否指parse header，下载文件时，解析完header后需将data传递给callback，而非将body接收完
     */
    recvPacket: function(socket, expectedCommand, expectedBodyLength, callback, headerOnly) {
        var oriCallback = callback;
        // 收包完毕则关闭掉连接
        callback = function() {
            cleanup();
            if (is.function(oriCallback)) {
                oriCallback.apply(null, arguments);
            }
        } ;
        var headerBufferLen = protocol.HEADER_BYTE_LENGTH;
        var headerBuffer = new Buffer(headerBufferLen);
        // 已填充的length
        var headerBufferFilled = 0;
        // 解析后的header信息
        // {status: , bodyLength: }
        var header;
        var bodyBuffer;
        // 下1次要copy到bodyBuffer中的起始位置
        var bodyBufferStart = 0;

        socket.on('data', listener);

        function listener(data) {
            // --------------- 收到服务器发来消息
            // -------- 解析header
            if (!header) {
                // header parsed
                if (headerBufferFilled + data.length >= headerBufferLen) {
                    var len = headerBufferFilled + data.length;
                    // 只copy剩下的header部分
                    data.copy(headerBuffer, headerBufferFilled, 0, headerBufferLen - headerBufferFilled);
                    try {
                        header = _parseHeader(headerBuffer, expectedCommand, expectedBodyLength);
                        logger.debug('receive server packet header: %j', header);
                        // 无body时直接返回
                        if (header.bodyLength === 0) {
                            callback(null, header);
                            return;
                        }

                        if (headerOnly) {
                            oriCallback(null, header);
                            if (len > headerBufferLen) {
                                oriCallback(null, data.slice(headerBufferLen - headerBufferFilled));
                            }
                            return;
                        }

                        bodyBuffer = new Buffer(header.bodyLength);

                        // 还有body的数据则填充到body buffer中
                        if (len > headerBufferLen) {
                            data.copy(bodyBuffer, 0, headerBufferLen - headerBufferFilled);
                            bodyBufferStart = len - headerBufferLen;
                            // 读取完毕
                            if (bodyBufferStart >= header.bodyLength) {
                                callback(null, bodyBuffer);
                            }
                        }

                    } catch (err) {
                        callback(err);
                    }

                } else {
                    data.copy(headerBuffer, headerBufferFilled);
                    headerBufferFilled += data.length;
                }

            } else {
                // 交由外部处理
                if (headerOnly) {
                    oriCallback(null, data);
                    return;
                }
                // ---------- 解析body
                data.copy(bodyBuffer, bodyBufferStart);
                bodyBufferStart += data.length;
                // 读取完毕
                if (bodyBufferStart >= header.bodyLength) {
                    callback(null, bodyBuffer);
                }
            }
        };

        function cleanup() {
            socket.removeListener('data', listener);
            // 关闭连接
            protocol.closeSocket(socket);
        };
    },

    /**
     * 给服务器发送关闭指令，同时end socket
     * @param socket
     */
    closeSocket: function(socket) {
        socket.end(protocol.packHeader(protocol.FDFS_PROTO_CMD_QUIT, 0, 0));
    },

    /**
     * 封装只有fileId的包，下载和删除文件时使用
     * @param command
     * @param group
     * @param filename
     * @param charset
     */
    packFileId: function(command, group, filename, charset) {
        // --------- 封装header
        var fnLength = Buffer.byteLength(filename, charset);
        var bodyLength = protocol.FDFS_GROUP_NAME_MAX_LEN + fnLength;
        var header = protocol.packHeader(command, bodyLength, 0);

        // --------- 封装body
        var body = new Buffer(bodyLength);
        // 默认都填充上0
        body.fill(0);
        var groupBL = Buffer.byteLength(group, charset);
        body.write(group, 0, groupBL, charset);
        body.write(filename, protocol.FDFS_GROUP_NAME_MAX_LEN, fnLength, charset);

        return Buffer.concat([header, body]);
    },

    /**
     * 封装meta data
     * 注：返回的是string，由外部写入到buffer中
     * @param {Object} metaData
     * @return {String}
     */
    packMetaData: function(metaData) {
        var first = true;
        var result = '';

        Object.keys(metaData).forEach(function(key) {
            if (!first) {
                result += protocol.FDFS_RECORD_SEPERATOR;

            } else {
                first = false;
            }
            var value = metaData[key];
            result += key;
            result += protocol.FDFS_FIELD_SEPERATOR;
            result += value;
        });

        return result;
    },

    /**
     * raw meta data to structure
     * @param raw
     */
    parseMetaData: function(raw) {
        var result = {};
        var md = raw.split(protocol.FDFS_RECORD_SEPERATOR);
        md.forEach(function(item) {
            var arr = item.split(protocol.FDFS_FIELD_SEPERATOR);
            var key = helpers.trim(arr[0]);
            var value = helpers.trim(arr[1]);
            result[key] = value;
        });

        return result;
    }
});

function _parseHeader(headerBuffer, expectedCommand, expectedBodyLength) {
    // validate buffer length
    if (headerBuffer.length !== protocol.FDFS_PROTO_PKG_LEN_SIZE + 2) {
        throw new Error('receive packet size ' + headerBuffer.length + ' is not equal to the expected header size: ' + protocol.FDFS_PROTO_PKG_LEN_SIZE + 2);
    }

    // validate command
    var command = Number('0x' + headerBuffer.toString('hex', protocol.PROTO_HEADER_CMD_INDEX, protocol.PROTO_HEADER_CMD_INDEX + 1));
    if (expectedCommand !== command) {
        throw new Error('receive command: ' + command + ' is not equal to the expected command: ' + expectedCommand);
    }

    // 应用层错误
    var status = Number('0x' + headerBuffer.toString('hex', protocol.PROTO_HEADER_STATUS_INDEX, protocol.PROTO_HEADER_STATUS_INDEX + 1));
    if (status !== protocol.HEADER_STATUS_SUCCESS) {
        var err = new Error('receive packet errno is: ' + status);
        err.code = err.errno = status;
        throw err;
    }

    // validate body length
    var bodyLength = helpers.buffer2Number(headerBuffer, 0);
    if (expectedBodyLength != null && expectedBodyLength !== bodyLength) {
        throw new Error('receive packet body length: ' + bodyLength + ' is not equal to the expected: ' + expectedBodyLength);
    }

    return {
        status: protocol.HEADER_STATUS_SUCCESS,
        bodyLength: bodyLength
    };
}
