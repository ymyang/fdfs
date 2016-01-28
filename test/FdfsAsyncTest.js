/**
 * Created by yang on 2015/8/24.
 */
var fs = require('fs');
var FdfsClient = require('../index.js');

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
    it('upload', function(done) {
        this.timeout(0);
        fdfs.upload('d:/test.jpg').then(function(fileId) {
            console.log(fileId);
            done();
        }).catch(done);
    });

    it('buffer upload', function(done) {
        this.timeout(0);

        var file = 'e:/shou.jpg';
        var buf = fs.readFileSync(file);
        var opts = {
            ext: 'jpg'
        };
        fdfs.upload(buf, opts).then(function(fileId) {
            console.log(fileId);
            done();
        }).catch(done);
    });

    it('stream upload', function(done) {
        this.timeout(0);

        var file = 'e:/shou.jpg';
        var stream = fs.createReadStream(file);
        var opts = {
            size: fs.statSync(file).size,
            ext: 'jpg'
        };

        fdfs.upload(stream, opts).then(function(fileId) {
            console.log(fileId);
            done();
        }).catch(done);
    });

    it('getFileInfo', function(done) {
        var fileId = 'group1/M00/00/00/wKgBeFZmNP2AYYbSAAPm5H9JxDA114.jpg';
        fdfs.getFileInfo(fileId).then(function(fileInfo) {
            console.log(fileInfo);
            done();
        }).catch(done);
    });

    it('setMetaData', function(done) {
        var fileId = 'group1/M00/00/03/wKgBeFZijMuADtt6AABCS_WBsFQ960.jpg';
        var meta = {
            fileName : 'shou.jpg',
            fileId: 1234
        };
        fdfs.setMetaData(fileId, meta, 'M').then(function() {
            console.log('setMetaData');
            done();
        }).catch(done);
    });

    it('getMetaData', function(done) {
        var fileId = 'group1/M00/00/03/wKgBeFZijMuADtt6AABCS_WBsFQ960.jpg';
        fdfs.getMetaData(fileId).then(function(meta) {
            console.log(meta);
            done();
        }).catch(done);
    });

    it('del', function(done) {
        var fileId = 'group1/M00/00/03/wKgBeFZijMuADtt6AABCS_WBsFQ960.jpg';
        fdfs.del(fileId).then(function() {
            console.log('del');
            done();
        }).catch(done);
    });

    it.only('download', function(done) {
        this.timeout(0);
        var fileId = 'group1/M00/00/00/wKgBeFZmNP2AYYbSAAPm5H9JxDA114.jpg';
        var file = 'd:/temp.jpg';
        fdfs.download(fileId, file).then(function() {
            console.log('download');
            done();
        }).catch(done);
    });

    it('test uploadAppenderFile', function(done) {
        this.timeout(0);
        var buff = fs.readFileSync('d:/test.jpg');
        var b1 = buff.slice(0, 10240);
        var b2 = buff.slice(10240);
        console.log('buff', buff.length, ', b1:', b1.length, ', b2:', b2.length);

        fdfs.upload(b1, {method: 'uploadAppender', ext: 'jpg'}).then(function(fileId) {
            console.log(fileId);
            return fdfs.upload(b2, {method: 'append', fileId: fileId});
        }).then(function(r) {
            console.log('append:', r);
            done();
        }).catch(done);
    });

    it('test modifyFile', function(done) {
        this.timeout(0);
        var buff = fs.readFileSync('d:/test.jpg');
        var b1 = buff.slice(0, 10240);
        var b2 = buff.slice(10240);
        console.log('buff', buff.length, ', b1:', b1.length, ', b2:', b2.length);
        fdfs.upload(b1, {method: 'uploadAppender', ext: 'jpg'}).then(function(fileId) {
            console.log(fileId);
            return fdfs.upload(b2, {method: 'modify', fileId: fileId, offset: b1.length});
        }).then(function(r) {
            console.log('modify:', r);
            done();
        }).catch(done);
    });
});