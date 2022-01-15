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

# 按时间倒序查询

## 实现原理

倒序分为 3 个维度来实现，由内核层到用户层分别为：`TsFile`、`BatchData`、`数据集`.
1. `TsFile` 是用来存储所有原始数据的文件格式，在存储时分为 `顺序文件` 和 `乱序文件`.
`ChunkData` 是 `TsFile` 中的基本数据块，保存了具体某一个测点的数据，且**数据是按照时间升序存储的**.
2. `BatchData` 是基础数据交换结构，无论是从文件系统还是缓存读出的数据都会被转换成 `BatchData` 结构。
3. `数据集` 是封装根据用户输入的 `SQL` 语句的结果集格式，转换和封装 `BatchData` 中的数据。

实际系统需要考虑的方面会更多，但主要为以上 3 点。
下面分别介绍各个层面如何实现基于时间的倒序查询：

## TsFile
1.`TsFile` 在文件末尾存储了各个测点在文件中的最小时间（开始时间）和最大时间（结束时间），
所以当我们按照开始时间升序排列文件并读取数据时，是升序读取；当使用结束时间倒序排列文件
(**顺序文件也可以使用开始时间倒序排列**) 并读取数据时，是**伪倒序读取**（因为文件内依然是升序存储的）, 数据示例：

```
    按照开始时间升序排列 (1,6)
   +---TsFile1---+   +---TsFile2---+   
   | 1,2,3,4,5   |   | 6,7,8,9,10  |
   +-------------+   +-------------+
   
    按照结束降序排列 (10,5)
   +---TsFile2---+   +---TsFile1---+   
   | 6,7,8,9,10  |   | 1,2,3,4,5   |
   +-------------+   +-------------+              
```

2. 上面的示例只描述了顺序文件，乱序文件的特点是：文件中存储的数据与其他文件的数据存在相交，
不仅仅和顺序文件相交，乱序和乱序文件之间也可能相交。

```
   +---TsFile1---+     +---TsFile2---+   
   | 1,2,3,4,5   |     | 6,7,8,9,10  |
   +-------------+     +-------------+
      +--------------unseq1-----------+     
      | 2,11                          |   
      +-------------------------------+
         +-unseq2-+     
         | 3,4,5  |   
         +--------+
      
```

3. 对于**相交**的部分，需要使用`多路归并`查询出最终结果。具体处理细节可以参见 [SeriesReader](../DataQuery/SeriesReader.md).

4. 对于文件层而言，我们抽象出了`TimeOrderUtils`，用来完成对升序、降序不同实现的方法提取。

```java
 public interface TimeOrderUtils {
    //排序使用的字段，升序使用开始时间，降序使用结束时间。
    //TsFileResource 面对文件相关，Statistics 面向 Chunk 和 Page 相关
    long getOrderTime(Statistics statistics);
    long getOrderTime(TsFileResource fileResource);

    //判断是否相交使用的时间依据，升序使用结束时间，降序使用开始时间
    long getOverlapCheckTime(Statistics range);

    //根据提供的时间判断是否相交，left 为依据时间，right 为要判断的文件
    boolean isOverlapped(Statistics left, Statistics right);
    boolean isOverlapped(long time, Statistics right);
    boolean isOverlapped(long time, TsFileResource right);

    //对于有序文件的遍历规则，升序时正序遍历，降序时倒序遍历
    TsFileResource getNextSeqFileResource(List<TsFileResource> seqResources, boolean isDelete);

    //对于所有文件层的临时缓存排序规则的实现
    <T> Comparator<T> comparingLong(ToLongFunction<? super T> keyExtractor);

    //循环读取数据时判断是否到达了当前设置的停止点
    boolean isExcessEndpoint(long time, long endpointTime);

    //或许当前使用的排序方式
    boolean getAscending();
  }
```

5. 完成了上述步骤，倒序取出来的数据如下：

```
6,7,8,9,10,1,2,3,4,5
```

这是因为数据在 Chunk、Page、缓存中都是升序存储，所以转移到 `BatchData` 中查看就是示例中的样子。

## BatchData
1. 为了兼容原有的文件读取数据方式，所以在数据放到 `BatchData` 之后倒序读取集合里的数据就可以得到最终结果。

BatchData 类似一个二维数组的格式，封装了写入和读取的方法，对于倒序，抽象了下面的子类：
```java

public class DescReadBatchData extends BatchData {
  //判断是否还有值，正序时候判断是否读到写入的 size 位置，倒序就是判断是否读到了 0 的位置
  public boolean hasCurrent();
  //指针移动到下一个数据，正序为 index+1 操作， 倒序为 index-1 操作
  public void next();
  //重制指针位置，正序为重制到 0，倒序重制到数据写入位置
  public void resetBatchData();
  //就像是 Nio 中的 Buffer 类的 flip 操作，在数据写入完成时做的操作，为读取做好准备
  public BatchData flip();
  //使用具体时间获取值，正序时 index 从 0 到 size 遍历，倒序时从 size 到 0 遍历
  public Object getValueInTimestamp(long time);
}
```

2. 在需要倒序处理数据的位置构建`DescBatchData`类，为此，特意设计了一个`BatchDataFactory`类，
根据 ascending 参数判断生产哪个类型的 BatchData。

3. 因为`MergeReader`使用了`PageReader`，所以在降序模式的时候数据会被倒序读出来（如下）:
```
5 4 3 2 1 
```
这种情况会使得`BatchData`使用`getValueInTimestamp`方法变得困难（因为在正序模式的时候默认为越往后的数据时间越大，
倒序模式的时候越后时间越小），但是按照如上数据示例无论在哪种模式下，都是违反了期望的数据时间比较，所以，新增了一个
`getValueInTimestamp(long time, BiFunction<Long, Long, Boolean> compare)`方法，
让不同的使用场景抉择基于时间的数据查找方式。

4. 至此数据从磁盘和缓存中取出的数据都是严格按照时间排序的了。

## 数据集
1. 原始数据查询
```sql
select * from root order by time desc 
select * from root where root.ln.d1.s1>100 order by time desc
select * from root where time >100 order by time desc
```
原始数据查询中，只要向`SeriesReader`传递了正确的 ascending 参数就能得到正确的结果。

2.GroupBy
```sql
select max_time(*) from root group by ((0,10],2ms) order by time desc
```
如上所示的 Sql 中，groupBy 查询的区间分别为：
```
[0-2),[2-4),[4-6),[6-8),[8-10)
```
倒序查询只需要将时间计算的过程修改为：
```
[8-10),[6-8),[4-6),[2-4),[0-2)
```

>计算方法为：
>
> (（结束时间-开始时间）/步长值） 结果向上取整，得到的值为 GroupBy 中会发生的遍历次数（记为 i)。
>
> （步长 * (i-1) + 开始时间）  结果为第 i 次遍历的开始时间值。

对于 BatchData 中的数据大于每次遍历的结束时间时，进行 skip，然后对 `AggregateResult` 新增
minBound 计算限制，限制为第 i 次遍历的开始时间值，
**数据无论是正序还是倒序遍历对于`AggregateResult`的结果是等价的**.

3.GroupByFill
```sql
select last_value(s0) from root.ln.d1 group by ((0,5],1ms) fill (int32[Previous]) order by time desc
```

对于升序的插值计算，在 `GroupByFillDataSet` 中做了前值缓存，当本次查询的值不为空时，更新缓存；
为空时使用缓存中的数据。

对于降序查询，因为数据在倒序遍历，并不知道它的前值是多少，所以为 `GroupByEngineDataSet` 
抽象了 `peekNextNotNullValue` 方法，继续往前执行，一直查找到一个不为空的值返回。并将该值缓存到 
`GroupByFillDataSet` 中并遵从升序的补值方法，到当前的循环开始时间小于缓存的时间时缓存失效。

4.Last
在`LastQueryExecutor`中，一次性将所有的结果集封装在了一个 List 中，
当倒序时将 List 中的数据根据时间进行一次排序，就得到了结果，详情见`ListDataSet`.
