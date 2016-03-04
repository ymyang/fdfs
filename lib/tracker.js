/**
 * Created by yang on 2015/8/25.
 */
'use strict';

var Buffer = require('buffer').Buffer;
var logger = require('./logger.js');
var protocol = require('./protocol.js');
var Storage = require('./storage.js');
var helpers = require('./helpers.js');

module.exports = Tracker;

function Tracker(config, socket) {
    this.config = config;
    this.socket = socket;
    this._name = config.host + ':' + config.port;
}

// public methods

/**
 * query storage server to upload file
 * 获取指定group的storage实例，如果不指定则tracker server会随机返回1个
 * @param group
 */
Tracker.prototype.getStoreStorage = function(group) {
    var _self = this;
    // 验证group是否过长
    if (group && group.length > protocol.FDFS_GROUP_NAME_MAX_LEN) {
        reject(new Error('group name [' + group + '] is too long'));
        return;
    }
    logger.debug('get a upload storage server from tracker server: [%s]', _self._name);

    var socket = _self.socket;
    // 获取1个可用storage server 信息
    // 封装header并发送
    var command;
    var bodyLength;
    if (!group) {
        command = protocol.TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE;
        bodyLength = 0;

    } else {
        command = protocol.TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE;
        bodyLength = protocol.FDFS_GROUP_NAME_MAX_LEN;
    }
    var header = protocol.packHeader(command, bodyLength, 0);
    logger.debug('send header to tracker server [%s]', _self._name);
    socket.write(header);

    // 发送body
    if (group) {
        var body = new Buffer(bodyLength);
        // 默认都填充上0
        body.fill(0);
        var groupBL = Buffer.byteLength(group, _self.config.charset);
        body.write(group, 0, groupBL, _self.config.charset);
        logger.debug('send body to tracker server [%s]', _self._name);
        socket.write(body);
    }

    return protocol.recvPacket(
        socket,
        protocol.TRACKER_PROTO_CMD_RESP,
        protocol.TRACKER_QUERY_STORAGE_STORE_BODY_LEN
    ).then(function(body) {
        var storageConfig = _parseStorage(body, _self.config.charset, true);
        storageConfig.timeout = _self.config.timeout;
        storageConfig.charset = _self.config.charset;
        var storage = new Storage(storageConfig);
        logger.debug('get store storage server info: %j ', storage.config);
        return storage;
    });
};

/**
 * query which storage server to download the file
 * 若只传1个参数则认为是fileId
 *
 * * TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE
     # function: query which storage server to download the file
     # request body:
     @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
     @ filename bytes: filename

     # response body:
     @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
     @ FDFS_IPADDR_SIZE - 1 bytes: storage server ip address
     @ FDFS_PROTO_PKG_LEN_SIZE bytes: storage server port
 *
 * @param fileId
 */
Tracker.prototype.getFetchStorage = function(fileId) {
    logger.debug('get a fetch storage server from tracker server: [%s]', this._name);
    return this.getFetchOrUpdateStorage(protocol.TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE, fileId);
};

Tracker.prototype.getUpdateStorage = function(fileId) {
    logger.debug('get a update storage server from tracker server: [%s]', this._name);
    return this.getFetchOrUpdateStorage(protocol.TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE, fileId);
};

/**
 * # request body:
     @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
     @ filename bytes: filename
 
   # response body:
     @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
     @ FDFS_IPADDR_SIZE - 1 bytes: storage server ip address
     @ FDFS_PROTO_PKG_LEN_SIZE bytes: storage server port
 * @param command TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE or TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE
 * @param fileId
 * @private
 */
Tracker.prototype.getFetchOrUpdateStorage = function(command, fileId) {
    var _self = this;
    var gf = helpers.id2gf(fileId);
    var group = gf.group;
    var filename = gf.filename;
    logger.debug('group: %s, filename: %s', group, filename);

    var socket = _self.socket;
    var packet = protocol.packFileId(command, group, filename, _self.config.charset);
    socket.write(packet);

    return protocol.recvPacket(
        socket,
        protocol.TRACKER_PROTO_CMD_RESP,
        protocol.FDFS_GROUP_NAME_MAX_LEN + protocol.FDFS_IPADDR_SIZE - 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE
    ).then(function(body) {
        var storageConfig = _parseStorage(body, _self.config.charset);
        storageConfig.timeout = _self.config.timeout;
        storageConfig.charset = _self.config.charset;
        var storage = new Storage(storageConfig);
        logger.info('get storage server info: %j ', storage.config);
        return storage;
    });
};

// helper methods
/**
 *   @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
 *   @ FDFS_IPADDR_SIZE - 1 bytes: storage server ip address
 *   @ FDFS_PROTO_PKG_LEN_SIZE bytes: storage server port
 *   @1 byte: store path index on the storage server {可以没有这个字节}
 *
 * @param {Buffer} body
 * @param {String} charset
 * @param {Boolean} hasPathIndex
 * @private {Object}
 */
function _parseStorage(body, charset, hasPathIndex) {
    var result = {};

    var group = helpers.trim(body.toString(charset, 0, protocol.FDFS_GROUP_NAME_MAX_LEN));
    var ip = helpers.trim(body.toString(charset, protocol.FDFS_GROUP_NAME_MAX_LEN, protocol.FDFS_GROUP_NAME_MAX_LEN + protocol.FDFS_IPADDR_SIZE - 1));
    var port = Number('0x' + body.toString('hex',
        protocol.FDFS_GROUP_NAME_MAX_LEN + protocol.FDFS_IPADDR_SIZE - 1,
        protocol.FDFS_GROUP_NAME_MAX_LEN + protocol.FDFS_IPADDR_SIZE - 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE));

    result.group = group;
    result.host = ip;
    result.port = port;

    if (hasPathIndex &&
        body.length > protocol.FDFS_GROUP_NAME_MAX_LEN + protocol.FDFS_IPADDR_SIZE - 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE) {
        var storePathIndex = Number('0x' + body.toString('hex',
            protocol.FDFS_GROUP_NAME_MAX_LEN + protocol.FDFS_IPADDR_SIZE - 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE,
            protocol.FDFS_GROUP_NAME_MAX_LEN + protocol.FDFS_IPADDR_SIZE - 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE + 1));

        result.storePathIndex = storePathIndex;
    }

    return result;
}

