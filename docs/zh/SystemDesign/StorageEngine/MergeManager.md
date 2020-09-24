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

# 文件合并机制

## 设计原理

在 IoTDB 中，出于查询性能的需要，我们要对乱序文件和小文件进行合并，并且在不同系统中提供不同的最佳合并算法。

所以在 merge 部分我们抽象尽可能多的类和接口来方便更多不同的合并策略的实现。

## 总体思路

* 将文件的合并过程分为少量乱序文件快速合并与大量小文件合并两个阶段
    * 将LSM中的C0层内存结构拆分为乱序缓冲区与顺序缓冲区。对于C0层顺序缓冲区的数据，当其大小达到阈值时，将被直接刷写入C2层。对于C0层的乱序数据则只能被刷写入C1层，并等待LSM的文件合并
    * 设置乱序数据为C1层，第一阶段会将乱序数据合并为C2层的顺序数据。第一阶段完成后，第二阶段将顺序小文件合并为更规整化的顺序大文件为C3层或更多的Cn层
* 文件合并过程
    * 当一系列的 seq 以及 unseq 文件进行合并（当仅合并乱序文件时，不存在 unseq 文件），且并未超过时间及内存限制时，所有文件都将合并为一个命名为{时间戳}-{版本}-{merge次数+1}.tsfile.merge.{merge_strategy}的文件
    * 当超出了时间或内存限制，文件选择过程将被中断，当前已被选中的 seq 以及 unseq 文件则会进行如上合并形成一个文件
        * 时间限制是指在选择文件过程中花费的时间不能超过一个给定值，而非对合并过程耗时的估计，目的是防止文件过多时选择文件花费太多时间
        * 内存限制是指对已经被选择的文件在进行合并时消耗的最大内存进行估计，并使该估计值不会超过一个给定值，从而方式合并过程产生内存溢出
* 文件合并恢复
    * 恢复的时候会根据合并的分情况进行丢弃或继续合并，见下文提到的详细恢复流程

## 配置修改

* 新增配置 seq_merge_file_strategy，表示乱序文件合并策略
    * 现在有 INPLACE、SQUEEZE 两种策略
    * INPLACE 为普通的乱序合并策略，仅对乱序文件进行合并
    * SQUEEZE 为混合合并策略，既然进行乱序文件合并也进行小文件合并，其中小文件合并策略取决当时的小文件合并策略
* 新增配置 size_merge_file_strategy，表示小文件合并策略
    * 现在仅有 REGULARIZATION 一种策略
    * REGULARIZATION 为普通的小文件合并策略，根据当前配置进行单层的文件合并
* 新增配置 merge_size_selector_strategy，表示是根据 chunk 时间范围还是点的个数对小文件进行判定
    * 现有 POINT_RANGE、TIME_RANGE 两种策略
    * POINT_RANGE 为根据点的个数判断，关联参数为 chunk_merge_point_threshold
    * TIME_RANGE 为根据时间范围判断，关联参数为 merge_file_time_block
* 新增配置 merge_file_time_block，表示合并目标文件最小 chunk 的时间间隔大小
    * 假设 merge_size_selector_strategy = TIME_RANGE、merge_file_time_block = 100，这时系统中存在 chunk1（0~60）、chunk2（60~90）、chunk3（90~200），则 selector 仅会选择 chunk1、chunk2 合并 

## 调用过程

- 每一次 merge 会在用户 client 调用"merge"命令或根据配置中的 merge_interval_sec 定时进行
- merge 分为三个过程，包括选择文件(selector)，进行 merge ，以及 merge 过程中断后的恢复(recover)，其调用结构图如下
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/24886743/75313978-6c64b000-5899-11ea-8565-40b012f9c8a2.png">

## 相关代码

* org.apache.iotdb.db.engine.merge.strategy.overlapped.BaseOverLappedFileSelector

    文件选择过程的基类，规定了选择文件的基本框架，及在不同情况下计算文件内存消耗的方法，所有自定义的文件选择策略均需继承此类
    
* org.apache.iotdb.db.engine.merge.IRecoverMergeTask
  
    recover 过程的接口类，规定了 recoverMerge 接口，所有自定义的merge恢复策略均需继承此类

此外，每一个自定义的MergeTask均需继承Callable\<void\>接口，以保证可以被回调

* org.apache.iotdb.db.engine.merge.manage.MergeContext

    Merge 过程中的公用上下文类

* org.apache.iotdb.db.engine.merge.manage.MergeManager

    Merge 过程中的线程池类，管理了多个 merge task 的运行

* org.apache.iotdb.db.engine.merge.manage.MergeResource

    Merge 过程中的资源类，负责管理 merge 过程中的 files,readers,writers,measurementSchemas,modifications 等资源

## inplace 策略

### selector

在受限的内存和时间下，先依次选择 unseq 文件，每次直接根据 unseq 文件的时间范围选择与之重叠的 seq 文件
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/24886743/93209196-587a9000-f790-11ea-905d-237d3b01d229.png">

### merge

首先根据选出的 unseq 和 seq 文件，合并到对应的 chunk 中去，然后根据合并 chunk 的 series 数量多少判断是新建一个文件写入还是写入原文件（均保持文件名与原 seq 文件一致）
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/24886743/93209276-77792200-f790-11ea-9410-642305c3e17f.png">

## squeeze 策略

### selector

在受限的内存和时间下，先依次选择 unseq 文件，每次根据 unseq 文件的时间范围选择与之重叠的seq文件，然后按次序重试每一个 seq 文件，尽可能在内存和时间受限的情况下多取一些 seq 文件
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/24886743/93209331-8fe93c80-f790-11ea-9fc1-bf20127b2645.png">

### merge

乱序文件合并过程基本与 inplace 策略类似，但是有一点不一样的是，写入不再写入到原文件中，而是将本次取出的所有数据根据当前的小文件合并策略写入到多个新文件中，最后删除合并完的源文件

## regularization

### selector

在受限的内存和时间下，根据配置中的 merge_size_selector_strategy、merge_file_time_block、chunk_merge_point_threshold 选择合适的 seq 文件
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/24886743/93209400-aee7ce80-f790-11ea-958a-61d4fe8f4854.png">

### merge

根据此时的小文件合并策略合并选出的 seq 文件，遍历 series 并根据配置中的 merge_chunk_sub_thread_num 分多个子线程进行合并
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/24886743/93209447-bc9d5400-f790-11ea-8f7e-26256b7e0f88.png">

## merge中断后的恢复

merge 在系统突然关闭或者出现故障时，可能会被强行中断，此时系统会记录下被中断的 merge 并在下一次 StorageGroupProcessor 被创建时扫描 merge.log 文件，根据配置进行重新 merge，也就是 recover 过程

merge 会有以下几个状态，其中恢复过程均为优先放弃合并策略

### NONE
基本什么都没干
恢复时直接删除相应的 merge log，以等待下一次的手动或自动 merge

### MERGE_START
将要合并的文件以及 timeseries 已经被选出
恢复时删除对应的 merge 文件，清空选出的文件，对其他 merge 相关公用资源均做清空操作

### ALL_TS_MERGED
所有的 timeseries 已经被合并
恢复时直接进行 cleanUp ，并执行 merge 完成的回调操作

### MERGE_END
表面所有的文件已经被合并，此次merge已经完成
原则上不会在 merge log 中出现此状态