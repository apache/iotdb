<!--

```
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
```

-->

## TsFile整理工具

### 介绍

1. 整理工具可以帮助你整理已封口的tsFile文件和.mods文件，过滤掉删除的数据并生成新的tsFile。整理完后会将本地旧的.mods文件删除，若某tsFile里的数据被全部删除了，则会删除本地该tsFile文件和对应的.resource文件。

2. 整理工具只能针对v0.12版本的IOTDB来使用，即只能针对版本为V3的tsFile进行整理，若低于此版本则必须先用在线升级工具将tsFile升级到V3版本。
3. tsFile整理工具分为在线整理工具和离线整理工具，两者在整理的时候都会记录日志，供下次恢复先前整理失败的文件。在执行整理的时候会先去检测日志里是否存在失败的文件，有的话则优先整理它们。

### 1. 离线整理工具

离线整理工具是以命令行的方式来启动，该工具的启动脚本settle.bat和settle.sh在编译了server后会生成至server\target\iotdb-server-{version}\tools\tsFileToolSet目录中。在使用的时候必须保证IOTDB Server是停止运行的，否则会出现整理错误。具体使用方式详见用户手册。

#### 1.1 涉及的相关类

整理工具类：org.apache.iotdb.db.tools.settle.tsFileAndModSettleTool

### 2.在线整理工具

在线整理工具是当用户在IOTDB客户端输入了settle命令后，会在后台注册启动一个整理服务Settle Service，该服务会去寻找指定存储组下的所有tsFile文件，并为每个tsFile开启一个整理线程进行整理。下面讲解整理线程的工作流程。

#### 2.1 整理线程的工作流程

1. 首先对该旧tsFile文件**加读锁**，避免在重写的过程中该tsFile被删除，而在删除该tsFile的本地mods文件前允许用户对该tsFile进行查询操作。【该步骤在StorageGroupProcessor类的settle()方法里】

2. 往整理日志记录标记该文件的状态为1。

3. 对旧tsFile和其mods文件进行整理并重写到新tsFile文件（此时新tsFile和对应的新.resource文件已经生成并存放在临时目录下，位置为：“旧tsFile所在目录\新tsFile时间分区目录\”）。若该旧tsFile数据都已被删除了，则会直接删除本地tsFile及其.resource文件。注意：在删除tsFile文件的前后会加写锁并释放写锁。【该步骤在tsFileAndModSettleTool类的settleOnetsFileAndMod()方法里】

4. 往整理日志记录标记该文件的状态为2。

5. 对旧tsFile文件**加写锁**，阻塞用户的查询等操作。【该步骤在StorageGroupProcessor类的settletsFileCallBack()方法里】

6. 删除旧mods文件，然后将新tsFile和对应的新.resource文件移动到正确的目录下（即旧tsFile所在目录），此过程是先删除旧对应旧文件，再把新的移动过去。【该步骤在tsFileRewriteTool类的moveNewtsFile()方法里】

7. 清空内存中与该tsFile的相关缓存数据，即该被重写的tsFile在内存中涉及到的相关数据都应该被及时更新：

   (1) 需要clear cache (包括chunk cache和timeseriesmetadata cache)，防止因为Mods被删除而读出该tsFile已被删除的数据（脏数据）

   (2) 若整理后，此tsFile被删除，则要从StorageGroupProcessor的tsFileManagement里挪去该oldtsFileResource.

   【该步骤在StorageGroupProcessor类的settletsFileCallBack()方法里】

8. 对该tsFile**释放写锁**，允许用户对该文件进行查询操作，然后**释放读锁**，允许该文件被删除。
9. 往整理日志标记该文件的状态为3，settle结束！

#### 2.2 注意事项

​	在线整理工具在整理过程中阻塞了用户对该虚拟存储组下的数据有任何的删除操作，直到整理完毕。

#### 2.3 补充说明

1. OldtsFileResource对应的本地tsFile文件不存在的情况

   (1) 上一次settle重写的过程中，旧tsFile因数据都被删掉了而相关文件被删掉，还没完成settle过程就出现异常导致整理线程中断。于是下次启动后发现settle.txt日志里的该tsFile整理记录对应的本地文件不存在。

   (2) 上次settle重写过程中，重写完后在移动的过程中，旧tsFile被删除而新tsFile还未移动前该线程就意外中断了。

2. Settle重写NewtsFileResource的返回情况（即tsFileAndModSettleTool类的settleOnetsFileAndMod()方法的返回情况）

   (1) 若旧tsFile没有封口，则不进行重写，返回null。

   (2) 若旧tsFile没有mods文件，即没有删除记录，则不进行重写，返回null。

   (3) 进行了重写后，旧tsFile的所有数据都被删除了，就会删除本地该tsFile文件和.resource文件，并且不会生成新的tsFile，则返回null。

#### 2.4 涉及的相关类

1. 整理服务类：org.apache.iotdb.db.service.SettleService
2. 整理线程类：org.apache.iotdb.db.engine.settle.SettleTask
3. 整理日志类：org.apache.iotdb.db.engine.settle.SettleLog
4. 整理工具类：org.apache.iotdb.db.tools.settle.tsFileAndModSettleTool

#### 2.5 整理日志

1. 整理日志文件的路径在"data\system\settle\settle.txt"，它有一条条记录项，存放了某个tsFile的整理状态，每条记录项的格式为：<tsFile文件路径>,<整理状态>

2. 整理共有三个状态，每个状态代表的含义如下：

   (1) 状态1：代表该tsFile在整理中，还未生成新的tsFile

   (2) 状态2：代表该tsFile的新tsFile已经生成，即整理线程已经把数据整理到新tsFile里了，还未移动到正确的目录下。

   (3) 状态3：代表该tsFile已经整理完毕且整理成功，旧tsFile对应的.mods文件已被删除，新的tsFile和对应的.resource文件已经移动到正确的目录下。

3. 当此次整理服务里所有的tsFile都被整理完毕且整理成功，就会删除整理日志settle.txt文件。

4. 整理日志可用于在下次重启时恢复上次整理失败的那些tsFile文件，具体的步骤为：

   1) 整理服务SettleService启动时，会先去查找settle.txt文件是否存在，若存在则读取日志中记录的那些整理失败的tsFile，把他们加入整理列表里。

   2) 开始整理：

   ​	(1) 对于状态是1的文件，则会删除上次整理失败而残留的新tsFile文件，并进行重写整理写入到新tsFile，然后删除对应的旧mods文件，并把新的tsFile和.resource文件移动到正确的目录下。

   ​	(2) 对于状态是2的文件，说明其新tsFile和对应的.resource文件已经生成，则只要把它们移动到正确的目录下即可。