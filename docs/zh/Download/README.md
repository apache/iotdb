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
              <td>0.11.2</td>
              <td><a href="https://www.apache.org/dyn/closer.cgi/iotdb/0.11.2/apache-iotdb-0.11.2-bin.zip">可执行程序</a></td>
              <td><a href="https://downloads.apache.org/iotdb/0.11.2/apache-iotdb-0.11.2-bin.zip.sha512">SHA512</a></td>
              <td><a href="https://downloads.apache.org/iotdb/0.11.2/apache-iotdb-0.11.2-bin.zip.asc">ASC</a></td>
              <td><a href="https://www.apache.org/dyn/closer.cgi/iotdb/0.11.2/apache-iotdb-0.11.2-source-release.zip">源代码</a></td>
              <td><a href="https://downloads.apache.org/iotdb/0.11.2/apache-iotdb-0.11.2-source-release.zip.sha512">SHA512</a></td>
              <td><a href="https://downloads.apache.org/iotdb/0.11.2/apache-iotdb-0.11.2-source-release.zip.asc">ASC</a></td>
              <td><a href="https://raw.githubusercontent.com/apache/iotdb/release/0.11.2/RELEASE_NOTES.md">release notes</a></td>
        </tr>
		<tr>
            <td>0.10.1</td>
            <td><a href="https://www.apache.org/dyn/closer.cgi/iotdb/0.10.1-incubating/apache-iotdb-0.10.1-incubating-bin.zip">可执行程序</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.10.1-incubating/apache-iotdb-0.10.1-incubating-bin.zip.sha512">SHA512</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.10.1-incubating/apache-iotdb-0.10.1-incubating-bin.zip.asc">ASC</a></td>
            <td><a href="https://www.apache.org/dyn/closer.cgi/iotdb/0.10.1-incubating/apache-iotdb-0.10.1-incubating-source-release.zip">源代码</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.10.1-incubating/apache-iotdb-0.10.1-incubating-source-release.zip.sha512">SHA512</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.10.1-incubating/apache-iotdb-0.10.1-incubating-source-release.zip.asc">ASC</a></td>
            <td><a href="https://raw.githubusercontent.com/apache/iotdb/release/0.10.1/RELEASE_NOTES.md">release notes</a></td>
      </tr>

</table>

历史版本下载: [https://archive.apache.org/dist/iotdb/](https://archive.apache.org/dist/iotdb/)


**<font color=red>升级注意事项</font>**:

- 如何升级小版本 (例如，从 v0.11.0 to v0.11.2)?
  * 同一个大版本下的多个小版本是互相兼容的。
  * 只需要下载新的小版本， 然后修改其配置文件，使其与原有版本的设置一致。
  * 停掉旧版本进程，启动新版本即可。

- 如何从v0.11.x或v0.10.x 升级到 v0.12.x? 
  * 从0.11或0.10升级到0.12的过程与v0.9升级到v0.10类似，升级工具会自动进行数据文件的升级。
  * 停掉旧版本新数据写入。
  * 用CLI调用`flush`，确保关闭所有的TsFile文件.
  * 我们推荐提前备份数据文件（以及写前日志和mlog文件），以备回滚。
  * 下载最新版，解压并修改配置文件。将各数据目录都指向备份的或者v0.11或0.10原来使用的数据目录。 把0.11中的其他修改都放到0.12中。
  * 停止旧版本IoTDB的实例，启动v0.12的实例。IoTDB将后台自动升级数据文件格式。在升级过程中数据可以进行查询和写入。
    * 当日志中显示`All files upgraded successfully! ` 后代表升级成功。
    * __注意1：0.12的配置文件进行了较大改动，因此不要直接将原本的配置文件用于0.12__
    * __注意2: 由于0.12不支持从0.9或者更低版本升级，如果需要升级，请先升级到0.10版本__
    * __注意3: 在文件升级完成前，最好不要进行delete操作。如果删除某个存储组内的数据且该存储组内存在待升级文件，删除会失败。__
 
- 如何从v0.10.x 升级到 v0.11.x?
  * 0.10 与0.11的数据文件格式兼容，但写前日志等格式不兼容，因此需要进行升级（但速度很快）：
  * 停掉0.10的新数据写入。
  * 用CLI调用`flush`，确保关闭所有的TsFile文件.
  * 我们推荐提前备份写前日志和mlog文件，以备回滚。
  * 下载最新版，解压并修改配置文件。将各数据目录都指向备份的或者v0.10原来使用的数据目录。 
  * 停止v0.10的实例，启动v0.11的实例。IoTDB将自动升级不兼容的文件格式。
  * __注意：0.11的配置文件进行了较大改动，因此不要直接将0.10的配置文件用于0.11__


- 如何从v0.9.x 升级到 v0.10.x? 
  * 停掉0.9的新数据写入。
  * 用CLI调用`flush`，确保关闭所有的TsFile文件.
  * 我们推荐提前备份数据文件（以及写前日志和mlog文件），以备回滚。
  * 下载最新版，解压并修改配置文件。将各数据目录都指向备份的或者v0.9原来使用的数据目录。 
  * 停止v0.9的实例，启动v0.10的实例。IoTDB将自动升级数据文件格式。

- 如何从0.8.x 升级到 v0.9.x?
  * 我们推荐提前备份数据文件（以及写前日志和mlog文件），以备回滚。
  * 下载最新版，解压并修改配置文件。将各数据目录都指向备份的或者v0.8原来使用的数据目录。 
  * 停止v0.8的实例，启动v0.9.x的实例。IoTDB将自动升级数据文件格式。
  


# 所有版本

在 [Archive repository](https://archive.apache.org/dist/iotdb/)查看所有版本



# 验证哈希和签名

除了我们的发行版，我们还在* .sha512文件中提供了sha512散列，并在* .asc文件中提供了加密签名。  Apache Software Foundation提供了广泛的教程来 [验证哈希和签名](http://www.apache.org/info/verification.html)，您可以使用任何这些发布签名的[KEYS](https://downloads.apache.org/iotdb/KEYS)来遵循这些哈希和签名。
