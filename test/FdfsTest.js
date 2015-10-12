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
    defaultExt: '',
    charset: 'utf8'
});

describe('test fdfs', function() {
    it.only('upload', function(done) {
        fdfs.upload('d:/index', function(err, fileId) {
            if (err) {
                console.error(err);
            }
            console.info(fileId);
            done();
        });
    });

    it('getFileInfo', function(done) {
        var fileId = 'group1/M00/00/12/wKgBeFX2npCAKbhJAAPm5H9JxDA192.jpg';
        fdfs.getFileInfo(fileId, function(err, fileInfo) {
            if (err) {
                console.error(err);
            }
            console.info(fileInfo);
            done();
        });
    });

    it('setMetaData', function(done) {
        var fileId = 'group1/M00/00/12/wKgBeFX2npCAKbhJAAPm5H9JxDA192.jpg';
        var meta = {
            fileName : 'test.jpg',
            fileId: 123
        }
        fdfs.setMetaData(fileId, meta, 'M', function(err) {
            if (err) {
                console.error(err);
            }
            done();
        });
    });

    it('getMetaData', function(done) {
        var fileId = 'group1/M00/00/12/wKgBeFX2npCAKbhJAAPm5H9JxDA192.jpg';
        fdfs.getMetaData(fileId, function(err, meta) {
            if (err) {
                console.error(err);
            }
            console.info(meta);
            done();
        });
    });

    it('del', function(done) {
        var fileId = '';
        fdfs.del(fileId, function(err) {
            if (err) {
                console.error(err);
            }
            done();
        });
    });

    it('download', function(done) {
        var fileId = '';
        var file = '';
        fdfs.download(fileId, file, function(err) {
            if (err) {
                console.error(err);
            }
            done();
        });
    });

    it('test uploadAppenderFile', function(done) {
        this.timeout(0);
        var buff = fs.readFileSync('d:/test.jpg');
        var b1 = buff.slice(0, 10240);
        var b2 = buff.slice(10240);
        console.log('buff', buff.length, ', b1:', b1.length, ', b2:', b2.length);
        fdfs.upload(b1, {method: 'uploadAppender', ext: 'jpg'}, function(err, fileId) {
            if (err) {
                console.error(err);
                done();
                return;
            }
            console.info('fileId:', fileId);
            fdfs.upload(b2, {method: 'append', fileId: fileId}, function(err, filiId1) {
                if (err) {
                    console.error(err);
                    done();
                    return;
                }
                console.info('fileId1:', filiId1);
                done();
            });
        });
    });

    it('test modifyFile', function(done) {
        this.timeout(0);
        var buff = fs.readFileSync('d:/test.jpg');
        var b1 = buff.slice(0, 10240);
        var b2 = buff.slice(10240);
        console.log('buff', buff.length, ', b1:', b1.length, ', b2:', b2.length);
        fdfs.upload(b1, {method: 'uploadAppender', ext: 'jpg'}, function(err, fileId) {
            if (err) {
                console.error(err);
                done();
                return;
            }
            console.info('fileId:', fileId);
            fdfs.upload(b2, {method: 'modify', fileId: fileId, offset: b1.length}, function(err, filiId1) {
                if (err) {
                    console.error(err);
                    done();
                    return;
                }
                console.info('fileId1:', filiId1);
                done();
            });
        });
    });
});