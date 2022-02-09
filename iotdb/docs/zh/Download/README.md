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
# 发行版本

<table>
	<tr>
      <th>Version</th>
	    <th colspan="3">IoTDB Binaries</th>
	    <th colspan="3">IoTDB Sources</th>
	    <th>release notes</th>  
	</tr>
	<tr>
            <td rowspan="4">0.12.4</td>
            <td><a href="https://www.apache.org/dyn/closer.cgi/iotdb/0.12.4/apache-iotdb-0.12.4-all-bin.zip">All-in-one</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-all-bin.zip.sha512">SHA512</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-all-bin.zip.asc">ASC</a></td>
            <td rowspan="4"><a href="https://www.apache.org/dyn/closer.cgi/iotdb/0.12.4/apache-iotdb-0.12.4-source-release.zip">源代码</a></td>
            <td rowspan="4"><a href="https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-source-release.zip.sha512">SHA512</a></td>
            <td rowspan="4"><a href="https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-source-release.zip.asc">ASC</a></td>
            <td rowspan="4"><a href="https://raw.githubusercontent.com/apache/iotdb/v0.12.4/RELEASE_NOTES.md">release notes</a></td>
      </tr>
      <tr>
            <td><a href="https://www.apache.org/dyn/closer.cgi/iotdb/0.12.4/apache-iotdb-0.12.4-server-bin.zip">单机版</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-server-bin.zip.sha512">SHA512</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-server-bin.zip.asc">ASC</a></td>
      </tr>
      <tr>
            <td><a href="https://www.apache.org/dyn/closer.cgi/iotdb/0.12.4/apache-iotdb-0.12.4-cluster-bin.zip">集群版</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-cluster-bin.zip.sha512">SHA512</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-cluster-bin.zip.asc">ASC</a></td>
      </tr>
      <tr>
            <td><a href="https://www.apache.org/dyn/closer.cgi/iotdb/0.12.4/apache-iotdb-0.12.4-grafana-bin.zip">Grafana 连接器</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-grafana-bin.zip.sha512">SHA512</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-grafana-bin.zip.asc">ASC</a></td>
      </tr>
</table>

历史版本下载：[https://archive.apache.org/dist/iotdb/](https://archive.apache.org/dist/iotdb/)

**<font color=red>注意事项</font>**:

- 推荐修改的操作系统参数
  * 将 somaxconn 设置为 65535 以避免系统在高负载时出现 "connection reset" 错误。
    ```
    # Linux
    > sudo sysctl -w net.core.somaxconn=65535
   
    # FreeBSD or Darwin
    > sudo sysctl -w kern.ipc.somaxconn=65535
    ```

- 如何升级小版本 （例如，从 v0.11.0 to v0.11.2)?
  * 同一个大版本下的多个小版本是互相兼容的。
  * 只需要下载新的小版本， 然后修改其配置文件，使其与原有版本的设置一致。
  * 停掉旧版本进程，启动新版本即可。

- 如何从 v0.11.x 或 v0.10.x 升级到 v0.12.x? 
  * 从 0.11 或 0.10 升级到 0.12 的过程与 v0.9 升级到 v0.10 类似，升级工具会自动进行数据文件的升级。
  * **<font color=red>停掉旧版本新数据写入。</font>**
  * 用 CLI 调用`flush`，确保关闭所有的 TsFile 文件。
  * 我们推荐提前备份数据文件（以及写前日志和 mlog 文件），以备回滚。
  * 下载最新版，解压并修改配置文件。将各数据目录都指向备份的或者 v0.11 或 0.10 原来使用的数据目录。 把 0.11 中的其他修改都放到 0.12 中。
  * 停止旧版本 IoTDB 的实例，启动 v0.12 的实例。IoTDB 将后台自动升级数据文件格式。在升级过程中数据可以进行查询和写入。
    * 当日志中显示`All files upgraded successfully! ` 后代表升级成功。
    * __注意 1：0.12 的配置文件进行了较大改动，因此不要直接将原本的配置文件用于 0.12__
    * __注意 2: 由于 0.12 不支持从 0.9 或者更低版本升级，如果需要升级，请先升级到 0.10 版本__
    * __注意 3: 在文件升级完成前，最好不要进行 delete 操作。如果删除某个存储组内的数据且该存储组内存在待升级文件，删除会失败。__
 
- 如何从 v0.10.x 升级到 v0.11.x?
  * 0.10 与 0.11 的数据文件格式兼容，但写前日志等格式不兼容，因此需要进行升级（但速度很快）：
  * **<font color=red>停掉 0.10 的新数据写入。</font>**
  * 用 CLI 调用`flush`，确保关闭所有的 TsFile 文件。
  * 我们推荐提前备份写前日志和 mlog 文件，以备回滚。
  * 下载最新版，解压并修改配置文件。将各数据目录都指向备份的或者 v0.10 原来使用的数据目录。 
  * 停止 v0.10 的实例，启动 v0.11 的实例。IoTDB 将自动升级不兼容的文件格式。
  * __注意：0.11 的配置文件进行了较大改动，因此不要直接将 0.10 的配置文件用于 0.11__

- 如何从 v0.9.x 升级到 v0.10.x? 
  * **<font color=red>停掉旧版本新数据写入。</font>**
  * 用 CLI 调用`flush`，确保关闭所有的 TsFile 文件。
  * 我们推荐提前备份数据文件（以及写前日志和 mlog 文件），以备回滚。
  * 下载最新版，解压并修改配置文件。将各数据目录都指向备份的或者 v0.9 原来使用的数据目录。 
  * 停止 v0.9 的实例，启动 v0.10 的实例。IoTDB 将自动升级数据文件格式。

- 如何从 0.8.x 升级到 v0.9.x?
  * 我们推荐提前备份数据文件（以及写前日志和 mlog 文件），以备回滚。
  * 下载最新版，解压并修改配置文件。将各数据目录都指向备份的或者 v0.8 原来使用的数据目录。 
  * 停止 v0.8 的实例，启动 v0.9.x 的实例。IoTDB 将自动升级数据文件格式。
  

# 所有版本

在 [Archive repository](https://archive.apache.org/dist/iotdb/) 查看所有版本

# 验证哈希和签名

除了我们的发行版，我们还在 *.sha512 文件中提供了 sha512 散列，并在 *.asc 文件中提供了加密签名。  Apache Software Foundation 提供了广泛的教程来 [验证哈希和签名](http://www.apache.org/info/verification.html)，您可以使用任何这些发布签名的 [KEYS](https://downloads.apache.org/iotdb/KEYS) 来遵循这些哈希和签名。
