/**
 * Created by yang on 2015/8/25.
 */
'use strict';

var Buffer = require('buffer').Buffer;
var Promise = require('bluebird');
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

    return new Promise(function(resolve, reject) {
        protocol.recvPacket(
            socket,
            protocol.TRACKER_PROTO_CMD_RESP,
            protocol.TRACKER_QUERY_STORAGE_STORE_BODY_LEN,
            function(err, body) {
                if (err) {
                    reject(err);
                    return;
                }
                var storageConfig = _parseStorage(body, _self.config.charset, true);
                storageConfig.timeout = _self.config.timeout;
                storageConfig.charset = _self.config.charset;
                var storage = new Storage(storageConfig);
                logger.debug('get store storage server info: %j ', storage.config);
                resolve(storage);
            }
        );
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

    return new Promise(function(resolve, reject) {
        protocol.recvPacket(
            socket,
            protocol.TRACKER_PROTO_CMD_RESP,
            protocol.FDFS_GROUP_NAME_MAX_LEN + protocol.FDFS_IPADDR_SIZE - 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE,
            function(err, body) {
                if (err) {
                    reject(err);
                    return;
                }
                var storageConfig = _parseStorage(body, _self.config.charset);
                storageConfig.timeout = _self.config.timeout;
                storageConfig.charset = _self.config.charset;
                var storage = new Storage(storageConfig);
                logger.info('get storage server info: %j ', storage.config);
                resolve(storage);
            }
        );
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

Tracker.prototype.listGroups = function() {
    var _self = this;
    var socket = _self.socket;

    // 封装header并发送
    var command = protocol.TRACKER_PROTO_CMD_SERVER_LIST_GROUP;
    var header = protocol.packHeader(command);
    logger.debug('send header to tracker server [%s]', _self._name);

    socket.write(header);

    return new Promise(function(resolve, reject) {
        protocol.recvPacket(
            socket,
            protocol.TRACKER_PROTO_CMD_RESP,
            null,
            function(err, body) {
                if (err) {
                    reject(err);
                    return;
                }
                if (body.length % 105 != 0) {
                    reject('byte array length:' + body.length + ' is invalid!');
                    return;
                }
                var res = {
                    count: body.length / 105,
                    groups: []
                };
                var charset = _self.config.charset;
                var offset = 0;
                for (var i = 0; i < res.count; i++) {
                    offset = 105*i;
                    res.groups.push({
                        groupName: helpers.trim(body.toString(charset, offset, offset + protocol.FDFS_GROUP_NAME_MAX_LEN)),
                        totalMB: helpers.buffer2Number(body, protocol.FDFS_GROUP_NAME_MAX_LEN + 1),
                        freeMB: helpers.buffer2Number(body, protocol.FDFS_GROUP_NAME_MAX_LEN + 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE*1),
                        trunkFreeMB: helpers.buffer2Number(body, protocol.FDFS_GROUP_NAME_MAX_LEN + 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE*2),
                        storageCount: helpers.buffer2Number(body, protocol.FDFS_GROUP_NAME_MAX_LEN + 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE*3),
                        storagePort: helpers.buffer2Number(body, protocol.FDFS_GROUP_NAME_MAX_LEN + 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE*4),
                        storageHttpPort: helpers.buffer2Number(body, protocol.FDFS_GROUP_NAME_MAX_LEN + 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE*5),
                        activeCount: helpers.buffer2Number(body, protocol.FDFS_GROUP_NAME_MAX_LEN + 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE*6),
                        currentWriteServer: helpers.buffer2Number(body, protocol.FDFS_GROUP_NAME_MAX_LEN + 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE*7),
                        storePathCount: helpers.buffer2Number(body, protocol.FDFS_GROUP_NAME_MAX_LEN + 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE*8),
                        subdirCountPerPath: helpers.buffer2Number(body, protocol.FDFS_GROUP_NAME_MAX_LEN + 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE*9),
                        currentTrunkFileId: helpers.buffer2Number(body, protocol.FDFS_GROUP_NAME_MAX_LEN + 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE*10),
                    });
                }
                resolve(res);
            }
        );
    });
};

Tracker.prototype.listStorages = function(group) {
    var _self = this;
    var socket = _self.socket;

    // 封装header并发送
    var command = protocol.TRACKER_PROTO_CMD_SERVER_LIST_STORAGE;
    var bodyLength = protocol.FDFS_GROUP_NAME_MAX_LEN;
    var header = protocol.packHeader(command, bodyLength);

    logger.debug('send header to tracker server [%s]', _self._name);
    socket.write(header);

    var body = new Buffer(bodyLength);
    // 默认都填充上0
    body.fill(0);
    var groupBL = Buffer.byteLength(group, _self.config.charset);
    body.write(group, 0, groupBL, _self.config.charset);
    logger.debug('send body to tracker server [%s]', _self._name);
    socket.write(body);

    return new Promise(function(resolve, reject) {
        protocol.recvPacket(
            socket,
            protocol.TRACKER_PROTO_CMD_RESP,
            null,
            function(err, body) {
                if (err) {
                    reject(err);
                    return;
                }
                if (body.length % 600 != 0) {
                    reject('byte array length:' + body.length + ' is invalid!');
                    return;
                }
                var res = {
                    count: body.length / 600,
                    storages: []
                };
                var charset = _self.config.charset;
                var offset = 0;
                for (var i = 0; i < res.count; i++) {
                    offset = 600*i;
                    var s = {};

                    s.status = body[offset];
                    offset += 1;
                    s.id = helpers.trim(body.toString(charset, offset, protocol.FDFS_STORAGE_ID_MAX_SIZE));
                    offset += protocol.FDFS_STORAGE_ID_MAX_SIZE;
                    s.ipAddr = helpers.trim(body.toString(charset, offset, offset + protocol.FDFS_IPADDR_SIZE));
                    offset += protocol.FDFS_IPADDR_SIZE;
                    s.domainName = helpers.trim(body.toString(charset, offset, offset + protocol.FDFS_DOMAIN_NAME_MAX_SIZE));
                    offset += protocol.FDFS_DOMAIN_NAME_MAX_SIZE;
                    s.srcIpAddr = helpers.trim(body.toString(charset, offset, offset + protocol.FDFS_IPADDR_SIZE));
                    offset += protocol.FDFS_IPADDR_SIZE;
                    s.version = helpers.trim(body.toString(charset, offset, offset + protocol.FDFS_VERSION_SIZE));
                    offset += protocol.FDFS_VERSION_SIZE;
                    s.joinTime = new Date(helpers.buffer2Number(body, offset)*1000);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.upTime = new Date(helpers.buffer2Number(body, offset)*1000);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.totalMB = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.freeMB = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.uploadPriority = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.storePathCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.subdirCountPerPath = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.currentWritePath = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.storagePort = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.storageHttpPort = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.totalUploadCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.successUploadCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.totalAppendCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.successAppendCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.totalModifyCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.successModifyCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.totalTruncateCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.successTruncateCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.totalSetMetaCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.successSetMetaCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.totalDeleteCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.successDeleteCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.totalDownloadCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.successDownloadCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.totalGetMetaCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.successGetMetaCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.totalCreateLinkCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.successCreateLinkCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.totalDeleteLinkCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.successDeleteLinkCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.totalUploadBytes = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.successUploadBytes = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.totalAppendBytes = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.successAppendBytes = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.totalModifyBytes = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.successModifyBytes = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.totalDownloadloadBytes = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.successDownloadloadBytes = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.totalSyncInBytes = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.successSyncInBytes = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.totalSyncOutBytes = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.successSyncOutBytes = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.totalFileOpenCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.successFileOpenCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.totalFileReadCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.successFileReadCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.totalFileWriteCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.successFileWriteCount = helpers.buffer2Number(body, offset);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.lastSourceUpdate = new Date(helpers.buffer2Number(body, offset)*1000);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.lastSyncUpdate = new Date(helpers.buffer2Number(body, offset)*1000);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.lastSyncedTimestamp = new Date(helpers.buffer2Number(body, offset)*1000);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.lastHeartBeatTime = new Date(helpers.buffer2Number(body, offset)*1000);
                    offset += protocol.FDFS_PROTO_PKG_LEN_SIZE;
                    s.ifTrunkServer = body[offset] != 0;

                    res.storages.push(s);
                }
                resolve(res);
            }
        );
    });
};
