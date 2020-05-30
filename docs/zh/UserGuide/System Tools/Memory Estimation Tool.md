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

# 内存预估
<!-- TOC -->

- [内存预估工具](#内存预估工具)
- [介绍](#介绍)
- [输入参数](#输入参数)
- [使用方式](#使用方式)

<!-- /TOC -->

# 介绍
本工具通过用户输入的若干参数，计算出IoTDB运行此负载的最小写内存。(IoTDB中的内存分为三部分：写内存，读内存，预留内存。写内存是用于数据写入分配的内存，三者的比例可在配置文件中设置)，结果以GB为单位。

# 输入参数
本工具使用时，需要输入的参数如下:
<table>
   <tr>
      <td>参数名</td>
      <td>参数说明</td>
      <td>示例</td>
      <td>是否必需</td>
   </tr>
   <tr>
      <td>-sg | --storagegroup &lt;storage group number&gt;</td>
      <td>存储组数量</td>
      <td>-sg 20</td>
      <td>是</td>
   </tr>
   <tr>
      <td>-ts | --timeseries &lt;total timeseries number&gt;</td>
      <td>总时间序列数量</td>
      <td>-ts 10000</td>
      <td>是</td>
   </tr>
   <tr>
      <td>-mts | --maxtimeseries &lt;max timeseries&gt;</td>
      <td>存储组中的最大时间序列的数量，如果时间序列均匀分配在存储组中，本参数可以不设置</td>
      <td>-mts 10000</td>
      <td>否</td>
   </tr>
</table>

在内存预估时，若工具计算需要较长的时间，则会在下方显示出运行进度，便于用户掌握进度。

# 使用方式

用户可以使用```$IOTDB_HOME/bin```文件夹下的脚本使用该工具
Linux系统与MacOS系统启动命令如下：
* 以20个存储组，共10w条时间序列，时间序列在存储组中均分为例：
```
  Shell >$IOTDB_HOME/bin/memory-tool.sh calmem -sg 20 -ts 100000
```
* 以20个存储组，共10w条时间序列，存储组中最大时间序列数为50000为例：
```
  Shell >$IOTDB_HOME/bin/memory-tool.sh calmem -sg 20 -ts 100000 -mts -50000
```

Windows系统启动命令如下：
* 以20个存储组，共10w条时间序列，时间序列在存储组中均分为例：
```
  Shell >$IOTDB_HOME\bin\memory-tool.bat calmem -sg 20 -ts 100000
```
* 以20个存储组，共10w条时间序列，存储组中最大时间序列数为50000为例：
```
  Shell >$IOTDB_HOME\bin\memory-tool.bat calmem -sg 20 -ts 100000 -mts -50000
```

