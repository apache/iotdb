<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# nifi-iotdb-bundle

## Apache NiFi简介

Apache NiFi 是一个易用的、功能强大的、可靠的数据处理和分发系统。

Apache NiFi 支持强大的、可伸缩的数据路由、转换和系统中介逻辑的有向图。

Apache NiFi 包含以下功能：

* 基于浏览器的用户接口：
    * 设计、控制、反馈和监控的无缝体验
* 数据起源跟踪
    * 从头到尾完整的信息族谱
* 丰富的配置
    * 丢失容忍和保证交付
    * 低延迟和高吞吐
    * 动态优先级策略
    * 运行时可以修改流配置
    * 反向压力控制
* 扩展设计
    * 用于定制 processors 和 services 的组件体系结构
    * 快速开发和迭代测试
* 安全会话
    * 带有可配置认证策略的 HTTPS 协议
    * 多租户授权和策略管理
    * 包括TLS和SSH的加密通信的标准协议

## PutIoTDB

这是一个用于数据写入的处理器。它使用配置的 Record Reader 将传入 FlowFile 的内容读取为单独的记录，并使用本机接口将它们写入 Apache IoTDB。

### PutIoTDB的配置项

| 配置项        | 描述                                                         | 默认值 | 是否必填 |
| ------------- | ------------------------------------------------------------ | ------ | -------- |
| Host          | IoTDB 的主机名                                               | null   | true     |
| Port          | IoTDB 的端口                                                 | 6667   | true     |
| Username      | IoTDB 的用户名                                               | null   | true     |
| Password      | IoTDB 的密码                                                 | null   | true     |
| Record Reader | 指定一个 Record Reader controller service 来解析数据，并且推断数据格式。 | null   | true     |
| Schema        | IoTDB 需要的 schema 不能很好的被 NiFi 支持，因此你可以在这里自定义 schema。<br />除此之外，你可以通过这个方式设置编码和压缩类型。如果你没有设置这个配置，就会使用 Record Reader 推断的 schema。<br />这个配置可以通过 Attributes 的表达式来更新。 | null   | false    |
| Aligned       | 是否使用 aligned 接口？<br />这个配置可以通过 Attributes 的表达式来更新。 | false  | false    |
| MaxRowNumber  | 指定 tablet 的最大行数。<br />这个配置可以通过 Attributes 的表达式来更新。 | 1024   | false    |

### Flowfile 的推断数据类型

如果要使用推断类型，需要注意以下几点：

1. 输入的 flowfile 需要能被 `Record Reader` 读取。
2. flowfile的 schema 中必须包含 `Time` 列，而且 `Time` 列必须是第一列。
3. `Time`的数据类型只能是 `STRING`  或者  `LONG `。
4. 除`Time` 以外的列必须以 `root.` 开头。
5. 支持的数据类型有： `INT`，`LONG`， `FLOAT`， `DOUBLE`， `BOOLEAN`， `TEXT`。

### 通过配置项自定义 schema

如上所述，通过配置项来自定义 schema 比起推断的 schema来说，是一种更加灵活和强大的方式。

 `Schema` 配置项的解构如下:

```json
{
   "timeType": "LONG",
   "fields": [{
      "tsName": "root.sg.d1.s1",
      "dataType": "INT32",
      "encoding": "RLE",
      "compressionType": "GZIP"
   }, {
      "tsName": "root.sg.d1.s2",
      "dataType": "INT64",
      "encoding": "RLE",
      "compressionType": "GZIP"
   }]
}
```

**注意**

1. flowfile 的第一列数据必须为 `Time`。剩下的必须与 `fields` 配置中保持一样的顺序。
1. 定义 shema 的 JSON 中必须包含 `timeType` and `fields` 这两项。
2. `timeType` 只支持 `LONG` 和 `STRING` 这两个选项。
3. `tsName` 和 `dataType` 这两项必须被设置。
4. `tsName` 必须以 `root.` 开头。 
5. 支持的 `dataTypes` 有：`INT32`， `INT64`， `FLOAT`， `DOUBLE`， `BOOLEAN`， `TEXT`。
6. 支持的 `encoding` 有： `PLAIN`， `DICTIONARY`， `RLE`， `DIFF`， `TS_2DIFF`， `BITMAP`， `GORILLA_V1`， `REGULAR`， `GORILLA`。
7. 支持的 `compressionType` 有： `UNCOMPRESSED`， `SNAPPY`， `GZIP`， `LZO`， `SDT`， `PAA`， `PLA`， `LZ4`。

## Relationships

| relationship | 描述                    |
| ------------ | ----------------------- |
| success      | 数据能被正确的写入。    |
| failure      | schema 或者数据有异常。 |
