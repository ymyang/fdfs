/**
 * Author: chenboxiang
 * Date: 14-6-13
 * Time: 下午10:17
 */
'use strict'

var Buffer = require('buffer').Buffer
var net = require('net')
var EventEmitter = require('events').EventEmitter
var util = require('util')
var logger = require('./logger')
var protocol = require('./protocol')
var Storage = require('./storage')
var helpers = require('./helpers')
var is = require('is-type-of')


function Tracker(config) {
    EventEmitter.call(this)

    this.config = config
    this._name = config.host + ':' + config.port
}

// extends from EventEmitter
util.inherits(Tracker, EventEmitter)

// ---------------- private methods

Tracker.prototype._getConnection = function() {
    return this._newConnection()
}

Tracker.prototype._newConnection = function() {
    var self = this
    var socket = new net.Socket()
    logger.debug('connect to tracker server [%s]', this._name)
    socket.setTimeout(this.config.timeout)
    socket.connect(this.config.port, this.config.host)

    socket.on('error', function(err) {
        self.emit('error', err)
    })

    socket.on('timeout', function() {
        socket.destroy()
        self.emit('error', new Error('connect to tracker server [' + self._name + '] timeout.'))
    })

    socket.on('connect', function() {
        logger.debug('tracker server [%s] is connected', self._name)
    })

    return socket
}

// ---------------- public methods

/**
 * query storage server to upload file
 * 获取指定group的storage实例，如果不指定则tracker server会随机返回1个
 * @param group
 */
Tracker.prototype.getStoreStorage = function(group, callback) {
    // 验证group是否过长
    if (group && group.length > protocol.FDFS_GROUP_NAME_MAX_LEN) {
        throw new Error('group name [' + group + '] is too long')
    }
    logger.debug('get a upload storage server from tracker server: [%s]', this._name)

    var self = this
    var socket = this._getConnection()
    socket.on('connect', function() {
        // ----------- 获取1个可用storage server 信息
        // -------- 封装header并发送
        var command
        var bodyLength
        if (!group) {
            command = protocol.TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE
            bodyLength = 0

        } else {
            command = protocol.TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE
            bodyLength = protocol.FDFS_GROUP_NAME_MAX_LEN
        }
        var header = protocol.packHeader(command, bodyLength, 0)
        logger.debug('send header to tracker server [%s]', self._name)
        socket.write(header)

        // -------- 发送body
        if (group) {
            var body = new Buffer(bodyLength)
            // 默认都填充上0
            body.fill(0)
            var groupBL = Buffer.byteLength(group, self.config.charset)
            body.write(group, 0, groupBL, self.config.charset)
            logger.debug('send body to tracker server [%s]', self._name)
            socket.write(body)
        }
    })

    protocol.recvPacket(
        socket,
        protocol.TRACKER_PROTO_CMD_RESP,
        protocol.TRACKER_QUERY_STORAGE_STORE_BODY_LEN,
        function(err, body) {
            if (null != err) {
                callback(err)
                return
            }

            var storageConfig = _parseStorage(body, self.config.charset, true)
            storageConfig.timeout = self.config.timeout
            storageConfig.charset = self.config.charset
            var storage = new Storage(storageConfig)
            logger.debug('get store storage server info: %j ', storage.config)
            callback(null, storage)
        }
    )
}

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
 * @param group
 * @param filename
 */
Tracker.prototype.getFetchStorage = function(group, filename, callback) {
    logger.debug('get a fetch storage server from tracker server: [%s]', this._name)
    this.getFetchOrUpdateStorage(protocol.TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE, group, filename, callback)
}

Tracker.prototype.getUpdateStorage = function(group, filename, callback) {
    logger.debug('get a update storage server from tracker server: [%s]', this._name)
    this.getFetchOrUpdateStorage(protocol.TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE, group, filename, callback)
}

/**
 * # request body:
     @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
     @ filename bytes: filename
 
   # response body:
     @ FDFS_GROUP_NAME_MAX_LEN bytes: group name
     @ FDFS_IPADDR_SIZE - 1 bytes: storage server ip address
     @ FDFS_PROTO_PKG_LEN_SIZE bytes: storage server port
 * @param command TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE or TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE
 * @param group
 * @param filename
 * @param callback
 * @private
 */
Tracker.prototype.getFetchOrUpdateStorage = function(command, group, filename, callback) {
    if (is.function(filename)) {
        callback = filename
        var gf = helpers.id2gf(group)
        group = gf.group
        filename = gf.filename
    }
    logger.debug('group: %s, filename: %s', group, filename)
    var self = this
    var socket = this._getConnection()
    socket.on('connect', function() {
        var packet = protocol.packFileId(command, group, filename, self.config.charset)
        socket.write(packet)
    })

    protocol.recvPacket(
        socket,
        protocol.TRACKER_PROTO_CMD_RESP,
        protocol.FDFS_GROUP_NAME_MAX_LEN + protocol.FDFS_IPADDR_SIZE - 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE,
        function(err, body) {
            if (null != err) {
                callback(err)
                return
            }
            var storageConfig = _parseStorage(body, self.config.charset)
            storageConfig.timeout = self.config.timeout
            storageConfig.charset = self.config.charset
            var storage = new Storage(storageConfig)
            logger.info('get storage server info: %j ', storage.config)
            callback(null, storage)
        }
    )
}

// -------------- helper methods
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
    var result = {}

    var group = helpers.trim(body.toString(charset, 0, protocol.FDFS_GROUP_NAME_MAX_LEN))
    var ip = helpers.trim(body.toString(charset, protocol.FDFS_GROUP_NAME_MAX_LEN, protocol.FDFS_GROUP_NAME_MAX_LEN + protocol.FDFS_IPADDR_SIZE - 1))
    var port = Number('0x' + body.toString('hex',
        protocol.FDFS_GROUP_NAME_MAX_LEN + protocol.FDFS_IPADDR_SIZE - 1,
        protocol.FDFS_GROUP_NAME_MAX_LEN + protocol.FDFS_IPADDR_SIZE - 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE))

    result.group = group
    result.host = ip
    result.port = port

    if (hasPathIndex &&
        body.length > protocol.FDFS_GROUP_NAME_MAX_LEN + protocol.FDFS_IPADDR_SIZE - 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE) {
        var storePathIndex = Number('0x' + body.toString('hex',
            protocol.FDFS_GROUP_NAME_MAX_LEN + protocol.FDFS_IPADDR_SIZE - 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE,
            protocol.FDFS_GROUP_NAME_MAX_LEN + protocol.FDFS_IPADDR_SIZE - 1 + protocol.FDFS_PROTO_PKG_LEN_SIZE + 1))

        result.storePathIndex = storePathIndex
    }

    return result
}

module.exports = Tracker
