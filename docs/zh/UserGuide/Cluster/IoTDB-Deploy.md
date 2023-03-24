# 用户手册

IoTDB集群管理工具旨在解决分布式系统部署时操作繁琐、容易出错的问题，
主要包括预检查、部署、启停、清理、销毁、配置更新、以及弹性扩缩容等功能。
用户可通过一行命令实现集群一键部署和维护管理，极大地降低管理难度。
[下载地址](https://github.com/TimechoLab/iotdb-deploy)
## 环境依赖
IoTDB集群管理工具需要python3.8及以上版本，IoTDB 要部署的机器需要依赖jdk 8及以上版本、lsof 或者 netstat、unzip功能如果没有请自行安装，可以参考文档最后的一节环境所需安装命令。

## 部署方法
* 在根目录内输入以下指令后：
```bash
bash build.sh
```
即可在之后的 shell 内激活 iotd 关键词，并安装所需的 whl 包。

* 如果要在该 shell 内立刻激活该关键词，需要执行：
```bash
alias "iotd=python3 <main.py's absolute path>"
```

## 集群配置文件介绍
* 在`iotd/config` 目录下有集群配置的yaml文件，yaml文件名字就是集群名字yaml 文件可以有多个，为了方便用户配置yaml文件在iotd/config目录下面提供了`default_cluster.yaml`示例。
* yaml 文件配置由`global`、`confignode_servers`、`datanode_servers`、`grafana_servers`(功能待开发)四大部分组成
* global 是通用配置主要配置机器用户名密码、IoTDB本地安装文件、Jdk配置等。在`iotd/config`目录中提供了一个`default_cluster.yaml`样例数据，
用户可以复制修改成自己集群名字并参考里面的说明进行配置iotdb集群

例如要执行`default_cluster.yaml`检查命令则需要执行命令`iotd cluster check default_cluster`即可，
更多详细命令请参考下面命令列表。

| 参数 | 说明 |
| ---| --- | 
|iotdb_zip_dir|IoTDB 部署分发目录，如果值为空则从`iotdb_download_url`指定地址下载|
|iotdb_download_url|IoTDB 下载地址，如果`iotdb_zip_dir` 没有值则从指定地址下载|
|jdk_tar_dir|jdk 本地目录，可使用该 jdk 路径进行上传部署至目标节点。|
|jdk_deploy_dir|jdk 远程机器部署目录，会将 jdk 部署到目标节点该文件夹下最终部署完成的路径是`<jdk_deploy_dir>/jdk_iotdb`|
|user|ssh登陆部署机器的用户名|
|password|ssh登录的密码, 如果password未指定使用pkey登陆, 请确保已配置节点之间ssh登录免密钥|
|pkey|密钥登陆如果password 有值优先使用password否则使用pkey登陆|
|ssh_port|ssh登录端口|
|deploy_dir|iotdb 部署目录，会把 iotdb 部署到目标节点该文件夹下最终部署完成的路径是`<deploy_dir>/iotdb`|
|datanode-env.sh|对应`iotdb/config/datanode-env.sh`|
|confignode-env.sh|对应`iotdb/config/confignode-env.sh`|
|iotdb-common.properties|对应`iotdb/config/iotdb-common.properties`|


* confignode_servers 是部署IoTDB Confignodes配置，里面可以配置多个Confignode
默认将第一个启动的ConfigNode节点node1当作Seed-ConfigNode

| 参数 | 说明 |
| ---| --- | 
|name|Confignode 名称|
|deploy_dir|IoTDB config node 部署目录，注:该目录不能与下面的IoTDB data node部署目录相同|
|iotdb-confignode.properties|对应`iotdb/config/iotdb-confignode.properties`更加详细请参看`iotdb-confignode.properties`文件说明|
|cn_internal_address|对应iotdb/内部通信地址，对应`iotdb/config/iotdb-confignode.properties`中的`cn_internal_address`|
|cn_target_config_node_list|集群配置地址指向存活的ConfigNode,默认指向confignode_x，对应`iotdb/config/iotdb-confignode.properties`中的`cn_target_config_node_list`|
|cn_internal_port|内部通信端口，对应`iotdb/config/iotdb-confignode.properties`中的`cn_internal_port`|
|cn_consensus_port|对应`iotdb/config/iotdb-confignode.properties`中的`cn_consensus_port`|
|cn_data_dir|对应`iotdb/config/iotdb-confignode.properties`中的`cn_data_dir`|
|iotdb-common.properties|对应`iotdb/config/iotdb-common.properties`在`global`与`confignode_servers`同时配置值优先使用confignode_servers中的值|


* datanode_servers 是部署IoTDB Datanodes配置，里面可以配置多个Datanode

| 参数 | 说明 |
| ---| --- | 
|name|Datanode 名称|
|deploy_dir|IoTDB data node 部署目录，注:该目录不能与下面的IoTDB config node部署目录相同|
|iotdb-datanode.properties|对应`iotdb/config/iotdb-datanode.properties`更加详细请参看`iotdb-datanode.properties`文件说明|
|dn_rpc_address|datanode rpc 地址对应`iotdb/config/iotdb-datanode.properties`中的`dn_rpc_address`|
|dn_internal_address|内部通信地址，对应`iotdb/config/iotdb-datanode.properties`中的`dn_internal_address`|
|dn_target_config_node_list|集群配置地址指向存活的ConfigNode,默认指向confignode_x，对应`iotdb/config/iotdb-datanode.properties`中的`dn_target_config_node_list`|
|dn_rpc_port|datanode rpc端口地址，对应`iotdb/config/iotdb-datanode.properties`中的`dn_rpc_port`|
|dn_internal_port|内部通信端口，对应`iotdb/config/iotdb-datanode.properties`中的`dn_internal_port`|
|iotdb-common.properties|对应`iotdb/config/iotdb-common.properties`在`global`与`datanode_servers`同时配置值优先使用`datanode_servers`中的值|

* grafana_servers 是部署Grafana 相关配置
该模块暂不支持


## 命令格式
本工具的基本用法为：
```bash
iotd cluster <key> <cluster name> <params>(Optional)
```
* key 表示了具体的命令。

* cluster name 表示集群名称(即`iotd/config` 文件中yaml文件名字)。

* params 表示了命令的所需参数(选填)。

* 例如部署default_cluster集群的命令格式为：

```bash
iotd cluster deploy default_cluster
```

* 集群的功能及参数列表如下：

| 命令 | 功能 | 参数 |
| ---| --- | ---|
|check|检测集群是否可以部署|集群名称列表|
|clean|清理集群|集群名称|
|deploy|部署集群|集群名称|
|list|打印集群及状态列表|无|
|start|启动集群|集群名称,节点名称(可选)|
|stop|关闭集群|集群名称,节点名称(可选)|
|restart|重启集群|集群名称|
|show|查看集群信息，details字段表示展示集群信息细节|集群名称, details(可选)|
|destroy|销毁集群|集群名称|
|scaleout|集群扩容|集群名称|
|scalein|集群缩容|集群名称，-N，集群节点名字或集群节点ip+port|
|reload|集群热加载|集群名称|
|distribute|集群配置文件分发|集群名称|
|run|一键执行集群检查、部署、启动|集群名称|

## 详细命令执行过程

下面的命令都是以default_cluster.yaml 为示例执行的，用户可以修改成自己的集群文件来执行

### 检查集群部署环境命令
```bash
iotd cluster check default_cluster
```

* 根据 cluster-name 找到默认位置的 yaml 文件，获取`confignode_servers`和`datanode_servers`配置信息

* 验证目标节点是否能够通过 SSH 登录

* 验证对应节点上的 JDK 版本是否满足IoTDB jdk1.8及以上版本、服务器是否按照unzip、是否安装lsof 或者netstat 

* 如果看到下面提示`Info:example check successfully!` 证明服务器已经具备安装的要求，
如果输出`Warn:example check fail!` 证明有部分条件没有满足需求可以查看上面的Warn日志进行修复，假如jdk没有满足要求，我们可以自己在yaml 文件中配置一个jdk1.8 及以上版本的进行部署不影响后面使用，如果检查lsof、netstat或者unzip 不满足要求需要在服务器上自行安装


### 部署集群命令

```bash
iotd cluster deploy default_cluster
```

* 根据 cluster-name 找到默认位置的 yaml 文件，获取`confignode_servers`和`datanode_servers`配置信息

* 根据`confignode_servers` 和`datanode_servers`中的节点信息上传iotdb压缩包和jdk压缩包(如果yaml中配置`jdk_tar_dir`和`jdk_deploy_dir`值)

* 根据yaml文件节点配置信息生成并上传`iotdb-common.properties`、`iotdb-confignode.properties`、`iotdb-datanode.properties`

提示：这里的confignode 和datanode部署到同一台机器上时目录不能为相同，否则会被后部署的节点文件覆盖


### 启动集群命令
```bash
iotd cluster check default_cluster
```

* 根据 cluster-name 找到默认位置的 yaml 文件，获取`confignode_servers`和`datanode_servers`配置信息

* 启动confignode，根据yaml配置文件中`confignode_servers`中的顺序依次启动同时根据进程id检查confignode是否正常，第一个confignode 为seek config

* 启动datanode，根据yaml配置文件中`datanode_servers`中的顺序依次启动同时根据进程id检查datanode是否正常

* 如果根据进程id检查进程存在后，通过cli依次检查集群列表中每个服务是否正常，如果cli链接失败则每隔10s重试一次直到成功最多重试5次


*启动单个节点命令*
```bash
iotd cluster start default_cluster datanode_1
```
or
```bash
iotd cluster start default_cluster 192.168.1.5:6667
```
* 根据 cluster-name 找到默认位置的 yaml 文件

* 根据提供的节点名称或者ip:port找到对于节点位置信息,如果启动的节点是`data_node`则ip使用yaml 文件中的`dn_rpc_address`、port 使用的是yaml文件中datanode_servers 中的`dn_rpc_port`。
如果启动的节点是`config_node`则ip使用的是yaml文件中confignode_servers 中的`cn_internal_address` 、port 使用的是`cn_internal_port`

* 启动该节点

### 查看集群状态命令
```bash
iotd cluster show default_cluster
```
or
```bash
iotd cluster show default_cluster details
```
* 根据 cluster-name 找到默认位置的 yaml 文件，获取`confignode_servers`和`datanode_servers`配置信息

* 依次在datanode通过cli执行`show cluster details` 如果有一个节点执行成功则不会在后续节点继续执行cli直接返回结果


### 停止集群命令
```bash
iotd cluster stop default_cluster
```
* 根据 cluster-name 找到默认位置的 yaml 文件，获取`confignode_servers`和`datanode_servers`配置信息

* 根据`datanode_servers`中datanode节点信息，按照配置先后顺序依次停止datanode节点

* 根据`confignode_servers`中confignode节点信息，按照配置依次停止confignode节点


*停止单个节点命令*
```bash
iotd cluster stop default_cluster datanode_1
```
or
```bash
iotd cluster stop default_cluster 192.168.1.5:6667
```
* 根据 cluster-name 找到默认位置的 yaml 文件

* 根据提供的节点名称或者ip:port找到对于节点位置信息，如果停止的节点是`data_node`则ip使用yaml 文件中的`dn_rpc_address`、port 使用的是yaml文件中datanode_servers 中的`dn_rpc_port`。
如果停止的节点是`config_node`则ip使用的是yaml文件中confignode_servers 中的`cn_internal_address` 、port 使用的是`cn_internal_port`

* 停止该节点


### 清理集群数据命令
```bash
iotd cluster clean default_cluster
```

* 根据 cluster-name 找到默认位置的 yaml 文件，获取`confignode_servers`和`datanode_servers`配置信息

* 根据`confignode_servers`和`datanode_servers`中node节点信息，检查是否节点还在运行，
如果有任何一个节点正在运行则不会执行清理命令

* 根据`confignode_servers`和`datanode_servers`中node配置节点信息依次清理数据(数据目录)


### 重启集群命令
```bash
iotd cluster restart default_cluster
```
* 根据 cluster-name 找到默认位置的 yaml 文件，获取`confignode_servers`和`datanode_servers`配置信息

* 执行上述的停止集群命令(stop),然后执行启动集群命令(start) 具体参考上面的start 和stop 命令

### 集群缩容命令
```bash
iotd cluster scalein default_cluster -N ip:port
```
```bash
iotd cluster scalein default_cluster -N clustername
```
* 根据 cluster-name 找到默认位置的 yaml 文件，获取`confignode_servers`和`datanode_servers`配置信息

* 判断要缩容的confignode节点和datanode是否只剩一个，如果只剩一个则不能执行缩容

* 然后根据ip:port或者clustername 获取要缩容的节点信息，执行缩容命令，然后销毁该节点目录，如果缩容的节点是`data_node`则ip使用yaml 文件中的`dn_rpc_address`、port 使用的是yaml文件中datanode_servers 中的`dn_rpc_port`。
如果缩容的节点是`config_node`则ip使用的是yaml文件中confignode_servers 中的`cn_internal_address` 、port 使用的是`cn_internal_port`

   
 提示：目前一次仅支持一个节点缩容

### 集群扩容命令
```bash
iotd cluster scaleout default_cluster  <绝对路径/iotdb.zip>
```
* 根据 cluster-name 找到默认位置的 yaml 文件，获取`confignode_servers`和`datanode_servers`配置信息

* 找到要扩容的节点，执行上传iotdb压缩包和jdb包(如果yaml中配置`jdk_tar_dir`和`jdk_deploy_dir`值)并解压

* 根据yaml文件节点配置信息生成并上传`iotdb-common.properties`、`iotdb-confignode.properties`或`iotdb-datanode.properties`

* 执行启动该节点命令并校验节点是否启动成功


### 销毁集群命令
```bash
iotd cluster destroy default_cluster
```

* cluster-name 找到默认位置的 yaml 文件

* 根据`confignode_servers`和`datanode_servers`中node节点信息，检查是否节点还在运行，
如果有任何一个节点正在运行则停止销毁命令

* 根据`confignode_servers`和`datanode_servers`中node配置节点信息依次清理数据(⽇志⽬录，数据⽬录、iotdb部署的目录)

### 分发集群配置命令
```bash
iotd cluster distribute default_cluster
```

* 根据 cluster-name 找到默认位置的 yaml 文件，获取`confignode_servers`和`datanode_servers`配置信息

* 根据yaml文件节点配置信息生成并依次上传`iotdb-common.properties`、`iotdb-confignode.properties`、`iotdb-datanode.properties`到指定节点

### 热加载集群配置命令
```bash
iotd cluster reload default_cluster
```
* 根据 cluster-name 找到默认位置的 yaml 文件，获取`confignode_servers`和`datanode_servers`配置信息

* 根据yaml文件节点配置信息依次在cli中执行`load configuration`

### 一键启动集群配置命令
```bash
iotd cluster run default_cluster
```
* 该命令会执行环境检查、部署集群命令和iotdb 服务启动命令


## 系统结构
IoTDB集群管理工具主要由cluster、config、logs、tools、doc、bin目录组成。

* `cluster`和`tools`存放部署工具执行代码。

* `config`存放要部署的集群配置文件如果要使用集群部署工具需要修改里面的yaml文件。

* `logs` 存放部署工具日志，如果想要查看部署工具执行日志请查看`logs/iotd.log`。

* `bin` 存放集群部署工具所需的二进制安装包，包括jdk、python和wheel。

* `doc` 存放用户手册、开发手册和推荐部署手册。

## 环境所需安装命令
* iotdb自身需要 linux 的 lsof 和 netstat 命令，所以需要预先安装

    Ubuntu: apt-get install lsof net-tools 
    
    Centos: yum install lsof net-tools
    
* iotd则需要unzip来解压iotdb的压缩包

    Ubuntu: apt-get install unzip
    
    Centos: yum install unzip
    
* Ubuntu20+的操作系统原生带有python3.7+，但是需要apt-get install python-pip，centos7+的操作系统只有python2，
    
    Ubuntu: apt-get install python3-pip
    
    Centos: 在同操作系统机器上进行编译之后拷贝，或者使用anaconda（700MB，解压之后4G）
