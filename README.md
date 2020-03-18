# elasticsearch-repository-ufile
插件可以将ES快照上传至ufile；在必要时可以对快照进行恢复，以便查询历史数据；   
插件需要在每个ES节点进行安装，并对安装好的ES节点进行重启。   

## 安装插件

### 通过elasticsearch-plugin安装

#### 本地安装

1. 下载`elasticsearch-repository-ufile-X.X.X.zip`。`X.X.X`为对应的elasticsearch版本号。[下载地址](https://github.com/ufilesdk-dev/elasticsearch-repository-ufile/releases)
2. `./<ES_HOME>/bin/elasticsearch-plugin install file:///<DOWNLOAD_PATH>/elasticsearch-repository-ufile-X.X.X.zip`
3. 安装插件时提示授权，输入`y`确认。

### 手工安装

1. 下载`elasticsearch-repository-ufile-X.X.X.zip`
2. 解压至`plugin`目录。`unzip /<DOWNLOAD_PATH>/elasticsearch-repository-ufile-X.X.X.zip -d /<ES_HOME>/plugin/`
3. 修改插件目录下的plugin-descriptor.properties里的对应版本至当前使用的es版本

## 使用方法

### 创建仓库

在对elasticsearch进行快照或恢复前，需要创建一个仓库。

```
PUT _snapshot/<1> 
{
    "type": "ufile",
    "settings": {
        "endpoint": <2>,
        "public_key": "xxxx", 
        "private_key": "xxxx", 
        "bucket": <3>,
        "compress": <4>,
        "chunk_size": <5>,
        "base_path": <6>,
        "max_snapshot_bytes_per_sec": <7>,
        "max_restore_bytes_per_sec": <8>
    }
}
```

* <1>：es备份仓库名
* <2>：ufile中bucket的域名。
* <3>：ufile中bucket的名称。
* <4>：是否对备份的索引做压缩。
* <5>：上传文件的分片大小，默认为64MB。
* <6>：备份文件在bucket中的路径（前缀名称），默认为根路径（无前缀）。
* <7>：快照时的上传速度，默认40MB/s。
* <8>：从快照恢复时的下载速度，默认40MB/s。

一个完整的仓库创建请求示例如下：

```bash
curl -XPUT localhost:9200/_snapshot/my_backup -H 'Content-type':'application/json' -d'{"type": "ufile","settings": {"endpoint": "estest.cn-bj.ufileos.com","public_key": "TOKEN_XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX","private_key": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX","bucket": "estest", "compress": true, "chunk_size":"50mb", "base_path": "es", "max_snapshot_bytes_per_sec": "20mb", "max_restore_bytes_per_sec": "20mb"}}'
```

### 备份、恢复及状态查看

参考官方文档：[documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html)

## 常用命令汇总

* 查询一个仓库信息
```bash
curl -XGET localhost:9200/_snapshot/<repository>
```

* 查询已创建的仓库：
```bash
curl -XGET localhost:9200/_snapshot?pretty
```

* 创建一个快照：
```bash
curl -XPUT localhost:9200/_snapshot/<repository>/<snapshot>
```

* 创建指定索引的一个快照
```bash
curl -XPUT localhost:9200/_snapshot/<repository>/<snapshot> -H 'Content-type':'application/json' -d'{"indices": "<index1>[,index2]..."}'
```

* 查询一个快照：
```bash
curl -XGET localhost:9200/_snapshot/<repository>/<snapshot>?pretty
```

* 查询一个仓库中所有快照
```bash
curl -XGET  localhost:9200/_snapshot/<repository>/_all
```

* 恢复一个快照：
```bash
curl -XPOST localhost:9200/_snapshot/<repository>/<snapshot>/_restore
```

* 删除一个快照：
```bash
curl -XDELETE localhost:9200/_snapshot/<repository>/<snapshot>
```

## 其他

### permission相关报错处理

在创建仓库时报例如java.net.NetPermission等permission相关错误。可以手工设定jvm启动参数来加载插件所需的java安全策略。
在`<ES_HOME>/config/jvm.options`中加上

```
-Djava.security.policy=/<ES_HOME>/plugins/elasticsearch-repository-ufile/plugin-security.policy
```
