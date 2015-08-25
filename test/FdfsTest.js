/**
 * Created by yang on 2015/8/24.
 */
var fs = require('fs');
var FdfsClient = require('../lib/fdfs');

var fdfs = new FdfsClient({
    trackers: [
        {
            host: '192.168.1.120',
            port: 22122
        }
    ],
    timeout: 10000,
    defaultExt: 'txt',
    charset: 'utf8'
})

describe('test fdfs', function() {
    it('test upload', function(done) {
        fdfs.upload('d:/test.jpg', function(err, fileId) {
            if (err) {
                console.error(err);
            }
            console.info(fileId);
            done();
        })
    });
    it('test uploadAppenderFile', function(done) {
        this.timeout(0);
        var buff = fs.readFileSync('d:/test.jpg');
        var b1 = buff.slice(0, 10240);
        var b2 = buff.slice(10240);
        console.log('buff', buff.length, ', b1:', b1.length, ', b2:', b2.length);
        fdfs.uploadAppenderFile(b1, {ext: 'jpg'}, function(err, fileId) {
            if (err) {
                console.error(err);
                done();
                return;
            }
            console.info('fileId:', fileId);
            var appenderFilename = fileId.substring(fileId.indexOf('/') + 1);
            console.info('appenderFilename:', appenderFilename);
            fdfs.appendFile(b2, {group: 'group1', appenderFilename: appenderFilename}, function(err, filiId1) {
                if (err) {
                    console.error(err);
                    done();
                    return;
                }
                console.info('fileId1:', filiId1);
                done();
            });
        })
    });
    it.only('test modifyFile', function(done) {
        this.timeout(0);
        var buff = fs.readFileSync('d:/test.jpg');
        var b1 = buff.slice(0, 10240);
        var b2 = buff.slice(10240);
        console.log('buff', buff.length, ', b1:', b1.length, ', b2:', b2.length);
        fdfs.uploadAppenderFile(b1, {ext: 'jpg'}, function(err, fileId) {
            if (err) {
                console.error(err);
                done();
                return;
            }
            console.info('fileId:', fileId);
            var appenderFilename = fileId.substring(fileId.indexOf('/') + 1);
            console.info('appenderFilename:', appenderFilename);
            fdfs.modifyFile(b2, {group: 'group1', appenderFilename: appenderFilename, offset: b1.length}, function(err, filiId1) {
                if (err) {
                    console.error(err);
                    done();
                    return;
                }
                console.info('fileId1:', filiId1);
                done();
            });
        })
    });
});