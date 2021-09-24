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
3. tsFile整理工具分为在线整理工具和离线整理工具，两者在整理的时候都会记录日志（"data\system\settle\settle.txt"），供下次恢复先前整理失败的文件。在执行整理的时候会先去检测日志里是否存在失败的文件，有的话则优先整理它们。

### 离线整理工具

该工具的启动脚本settle.bat和settle.sh在编译了server后会生成至server\target\iotdb-server-{version}\tools\tsFileToolSet目录中。运行脚本时要给定至少一个参数，该参数可以是一个目录路径或者是一个具体的tsFile文件路径，不同参数间用空格分割。若参数是目录，则离线整理工具会递归寻找该目录下所有已封口的tsFile进行整理。若有多个参数，离线整理工具则会寻找指定参数下的所有tsFile文件，对他们依次进行整理。

在使用离线整理工具的时候，必须保证IOTDB server停止运行，否则会出错。

#### 1. 运行方法

```bash
#Windows
.\settle.bat <目录路径/tsFile路径> <目录路径/tsFile路径> ...

#MacOs or Linux
./settle.sh <目录路径/tsFile路径> <目录路径/tsFile路径> ...
```

#### 2. 运行示例

```
>.\settle.bat C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0

​````````````````````````
Starting Settling the tsFile
​````````````````````````
Totally find 3 tsFiles to be settled, including 0 tsFiles to be recovered.
Start settling for tsFile : C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0\1631261328514-1-0-2.tsFile
Finish settling successfully for tsFile : C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0\1631261328514-1-0-2.tsFile
Start settling for tsFile : C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0\1631274465662-3-0-1.tsFile
Finish settling successfully for tsFile : C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0\1631274465662-3-0-1.tsFile
Start settling for tsFile : C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0\1631433121335-5-0-0.tsFile
Finish settling successfully for tsFile : C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0\1631433121335-5-0-0.tsFile
Finish settling all tsFiles successfully!
```

### 在线整理工具

当用户在IOTDB客户端输入了settle命令后，在线整理工具会在后台注册启动一个整理服务Settle Service，该服务会去寻找指定存储组下的所有tsFile文件，并为每个tsFile开启一个整理线程进行整理。在线整理工具在整理过程中不允许用户对该虚拟存储组下的数据有任何的删除操作，直到整理完毕。

#### 1. 运行方法

```
IoTDB> Settle <存储组>
```

#### 2. 运行示例

```
IoTDB> Settle root.ln;
Msg: The statement is executed successfully.
```