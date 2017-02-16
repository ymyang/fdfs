# Nodejs Client for FastDFS

[FastDFS](https://github.com/happyfish100/fastdfs) 是分布式文件存储系统。这个项目是FastDFS的NodeJS客户端，用来与FastDFS Server进行交互，进行文件的相关操作。我测试过的server版本是4.0.6。

# 安装

```shell

npm install fdfs

```

# 使用

```javascript

var FdfsClient = require('fdfs');

var fdfs = new FdfsClient({
    // tracker servers
    trackers: [
        {
            host: 'tracker.fastdfs.com',
            port: 22122
        }
    ],
    // 默认超时时间10s
    timeout: 10000,
    // 默认后缀
    // 当获取不到文件后缀时使用
    defaultExt: 'txt',
    // charset默认utf8
    charset: 'utf8'
});

```

以上是一些基本配置，你还可以自定义你的日志输出工具，默认是使用console
例如你要使用[debug](https://github.com/visionmedia/debug)作为你的日志输出工具，你可以这么做：

```javascript

var debug = require('debug')('fdfs');
var fdfs = new FdfsClient({
    // tracker servers
    trackers: [
        {
            host: 'tracker.fastdfs.com',
            port: 22122
        }
    ],
    logger: {
        log: debug
    }
});
```

### 上传文件

注：以下fileId为group + '/' + filename，以下的所有操作使用的fileId都是一样

通过本地文件名上传

```javascript

fdfs.upload('e:/shou.jpg').then(function(fileId) {
    // fileId 为 group + '/' + filename
    console.log(fileId);
}).catch(function(err) {
    console.error(err);
);

```

上传Buffer

```javascript

var fs = require('fs');

// 注意此处的buffer获取方式只为演示功能，实际不会这么去构建buffer
var buffer = fs.readFileSync('test.gif');
fdfs.upload(buffer).then(function(fileId) {
    // fileId 为 group + '/' + filename
    console.log(fileId);
}).catch(function(err) {
    console.error(err);
);

```

ReadableStream

```javascript

var fs = require('fs');

var rs = fs.createReadStream('test.gif');
fdfs.upload(rs).then(function(fileId) {
    // fileId 为 group + '/' + filename
    console.log(fileId);
}).catch(function(err) {
    console.error(err);
);

```

其他一些options，作为第2个参数传入

```js

fdfs.upload('test.gif', {
    // 上传方法 [upload, uploadAppender, append, modify], 默认为upload
    method: 'upload',
    // 指定文件存储的group，不指定则由tracker server分配
    group: 'group1',
    // method为append或modify指定追加的源文件
    fileId: 'group1/M00/00/0F/wKgBeFXlZJuAdsBZAAPm5H9JxDA153.jpg',
    // file bytes, file参数为ReadableStream时必须指定
    size: 1024,
    // method为modify指定追加的源文件的起始点
    offset: 10240,
    // 上传文件的后缀，不指定则获取file参数的后缀，不含(.)
    ext: 'jpg'
}).then(function(fileId) {
    // fileId 为 group + '/' + filename
    console.log(fileId);
}).catch(function(err) {
    console.error(err);
);
 
```

### 下载文件

下载到本地

```js

fdfs.download(fileId, 'test_download.gif').then(function() {
    // 下载完成
    
}).catch(function(err) {
    console.error(err);
);

```

下载到WritableStream

```js

var fs = require('fs');
var ws = fs.createWritableStream('test_download.gif');
fdfs.download(fileId, ws).then(function() {
    // 下载完成
    
}).catch(function(err) {
    console.error(err);
);

```

下载文件片段

```js

fdfs.download(fileId, {
    target: 'test_download.part',
    offset: 5,
    bytes: 5
}).then(function() {
    // 下载完成
    
}).catch(function(err) {
    console.error(err);
);

```

### 删除文件

```js

fdfs.del(fileId).then(function() {
    // 删除成功
    
}).catch(function(err) {
    console.error(err);
);

```

### 获取文件信息

```js

fdfs.getFileInfo(fileId).then(function(fileInfo) {
    // fileInfo有4个属性
    // {
    //   // 文件大小
    //   size:
    //   // 文件创建的时间戳，单位为秒
    //   timestamp:
    //   // 校验和
    //   crc32:
    //   // 最初上传到的storage server的ip
    //   addr:
    // }
    console.log(fileInfo);
}).catch(function(err) {
    console.error(err);
);

```

### 文件的Meta Data

设置Meta Data, 我只贴出来文件签名信息吧，flag字段如果不传则默认是O

```js

/**
 * @param fileId
 * @param metaData  {key1: value1, key2: value2}
 * @param flag 'O' for overwrite all old metadata (default)
                'M' for merge, insert when the meta item not exist, otherwise update it
 */
fdfs.setMetaData(fileId, metaData, flag).then(function() {
    // 设置成功
    
}).catch(function(err) {
    console.error(err);
); 

```

获取Meta Data

```js

fdfs.getMetaData(fileId).then(function(metaData) {
    console.log(metaData);
}).catch(function(err) {
    console.error(err);
);

```

### group信息

```js

fdfs.listGroups().then(function(groups) {
    console.log(groups);
}).catch(function(err) {
    console.error(err);
); 

```

### storage信息

```js

fdfs.listStorages(‘group1’).then(function(storages) {
    console.log(storages);
}).catch(function(err) {
    console.error(err);
); 

```