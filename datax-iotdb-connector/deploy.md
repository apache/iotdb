
# DataX iotdbwriter

[DataX](https://github.com/alibaba/DataX) iotdbwriter 插件，用于通过 DataX 同步其他数据源的数据到 Doris 中。

这个插件是利用IotDB session功能进行数据导入的。需要配合 DataX 服务一起使用。

## 关于 DataX

DataX 是阿里云 DataWorks数据集成 的开源版本，在阿里巴巴集团内被广泛使用的离线数据同步工具/平台。DataX 实现了包括 MySQL、Oracle、SqlServer、Postgre、HDFS、Hive、ADS、HBase、TableStore(OTS)、MaxCompute(ODPS)、Hologres、DRDS 等各种异构数据源之间高效的数据同步功能。

更多信息请参阅: `https://github.com/alibaba/DataX/`

## 使用手册

DataX iotdbwriter 插件代码 [这里](./iotdbwriter)。

这个目录包含插件代码以及 DataX 项目的开发环境。

iotdbwriter 插件依赖的 DataX 代码中的一些模块。而这些模块并没有在 Maven 官方仓库中。所以我们在开发 iotdbwriter 插件时，需要下载完整的 DataX 代码库，才能进行插件的编译和开发。

### 目录结构

1. `iotdbwriter/`

    这个目录是 iotdbwriter 插件的代码目录。这个目录中的所有代码，都托管在 Apache IotDB 的代码库中。

    iotdbwriter 插件帮助文档在这里：`iotdbwriter/doc`

2. `init-env.sh`

    这个脚本主要用于构建 DataX 开发环境，他主要进行了以下操作：
    
    1. 将 DataX 代码库 clone 到本地。
    2. 将 `iotdbwriter/` 目录软链到 `DataX/iotdbwriter` 目录。
    3. 在 `DataX/pom.xml` 文件中添加 `<module>iotdbwriter</module>` 模块。

    这个脚本执行后，开发者就可以进入 `DataX/` 目录开始开发或编译了。因为做了软链，所以任何对 `DataX/iotdbwriter` 目录中文件的修改，都会反映到 `iotdbwriter/` 目录中，方便开发者提交代码。

### 编译

1. 运行 `init-env.sh`
2. 按需修改 `DataX/iotdbwriter` 中的代码。
3. 编译 iotdbwriter：

    ` mvn -U clean package assembly:assembly -Dmaven.test.skip=true`        

    产出在 `target/datax/datax/`.

    如果编译出现插件依赖下载错误，如： hdfsreader, hdfswriter, tsdbwriter and oscarwriter 这几个插件，是因为需要额外的jar包。如果你并不需要这些插件，可以在 `DataX/pom.xml` 中删除这些插件的模块。


4. 按需提交修改。

### 示例

iotdbwriter 插件的使用说明请参阅 [这里](./iotdbwriter/doc/iotdbwriter.md)
