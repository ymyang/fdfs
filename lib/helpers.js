/**
 * Created by yang on 2015/8/25.
 */
'use strict';

var Buffer = require('buffer').Buffer;

var trimReg = /^\u0000+|\u0000+$/g;

module.exports = {
    /**
     * 将unsigned number转换为Big-Endian buffer
     * @param number
     * @param bytes buffer的字节数
     */
    number2Buffer: function(number, bytes) {
        if (number < 0) {
            throw new Error('"number" must greater than or equal to zero.');
        }
        bytes = bytes || 8;
        // 转换为16进制字符串
        var hex = number.toString(16);

        // ------- 将hex length补充成bytes x 2，不足则高位补0，超过则去掉高位
        var length = hex.length;
        var targetLength = bytes * 2;
        if (length < targetLength) {
            var i = targetLength - length;
            while (i > 0) {
                hex = '0' + hex;
                i--;
            }

        } else if (length > targetLength) {
            hex = hex.substring(length - targetLength);
        }

        // ------ 填充到buffer里，高位在前
        var buffer = new Buffer(bytes);
        var offset = 0;
        while (offset < bytes) {
            var bn = Number("0x" + hex.substring(offset * 2, (offset * 2) + 2));
            buffer.writeUInt8(bn, offset);
            offset++;
        }

        return buffer;
    },

    /**
     * 将buf转为数字
     * @param buf
     * @param offset
     * @returns {number}
     */
    buffer2Number: function(buf, offset) {
        var str = buf.toString('hex', offset, offset + 8);
        var v = parseInt(str.substring(8), 16) - parseInt(str.substring(0, 8), 16);
        if (v < 0) {
            v--;
        }
        return v;
    },

    /**
     * 构造Object.defineProperty中指定的property属性，并达到const声明的效果，只读，不可写
     * @param value
     * @returns {{configurable: boolean, writable: boolean, value: *}}
     */
    buildConstProp: function(value) {
        return {
            configurable: false,
            writable: false,
            value: value
        };
    },

    /**
     * 在String.prototype.trim的基础上再去掉\u0000
     *  默认trim的处理见下:
     *      https://developer.mozilla.org/en/JavaScript/Reference/Global_Objects/String/trim
     *      http://blog.stevenlevithan.com/archives/faster-trim-javascript
     *      http://jsperf.com/mega-trim-test
     * @param str
     * @returns {string}
     */
    trim: function(str) {
        return str.trim().replace(trimReg, '');
    },

    /**
     * file id conver to group and filename
     * @param fileId
     * @returns {{group: string, filename: string}}
     */
    id2gf: function(fileId) {
        var pos = fileId.indexOf('/');
        return {
            group: fileId.substring(0, pos),
            filename: fileId.substring(pos + 1)
        };
    }
};