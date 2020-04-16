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

# 降采样补空值查询

GroupByFill 查询的主要逻辑在 `GroupByFillDataSet`

* `org.apache.iotdb.db.query.dataset.groupby.GroupByFillDataSet`

GroupByFill是对原降采样结果进行填充，目前仅支持使用前值填充的方式。

* 在Group By Fill中，Group by子句不支持滑动步长，否则抛异常
* Fill子句中仅能使用Previous和PREVIOUSUNTILLAST这两种插值方式，Linear不支持
* Previous和PREVIOUSUNTILLAST对fill的时间不做限制
* 填充只针对last_value这一聚合函数，其他的函数不支持，如果其他函数的聚合值查询结果为null，依旧为null，不进行填充

## PREVIOUSUNTILLAST与PREVIOUS填充的区别

Previous填充方式的语意没有变，只要前面有值，就可以拿过来填充；
PREVIOUSUNTILLAST考虑到在某些业务场景下，所填充的值的时间不能大于该时间序列last的时间戳（从业务角度考虑，取历史数据不能取未来历史数据）
看下面的例子，或许更容易理解

A点时间戳为1，B为5，C为20，D为30，N为8，M为38



原始数据为<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/16079446/78784824-9f41ae00-79d8-11ea-9920-0825e081cae0.png">

`select temperature FROM root.ln.wf01.wt01 where time >= 1 and time <= 38`

| Time   | root.ln.wf01.wt01.temperature  |
| ------ | ------------------------------ |
| 1      | 21                             |
| 3      | 23                             |
| 5      | 25                             |
| 20     | 26                             |
| 27     | 29                             |
| 28     | 30                             |
| 30     | 40                             |


当我们使用Previous插值方式时，即使D到M这一段是未来的数据，我们也会用D点的数据进行填充

`SELECT last_value(temperature) as last_temperature FROM root.ln.wf01.wt01 GROUP BY([8, 39), 5m) FILL (int32[previous])`

| Time   | last_temperature |
| ------ | ---------------- |
| 8      | 25               |
| 13     | 25               |
| 18     | 26               |
| 23     | 29               |
| 28     | 40               |
| 33     | 40               |
| 38     | 40               |

当我们使用NONLASTPREVIOUS插值方式时，因为D到M这一段是未来的数据，我们不会进行插值，还是返回null

`SELECT last_value(temperature) as last_temperature FROM root.ln.wf01.wt01 GROUP BY([8, 39), 5m) FILL (int32[PREVIOUSUNTILLAST])`

| Time   | last_temperature |
| ------ | ---------------- |
| 8      | 25               |
| 13     | 25               |
| 18     | 26               |
| 23     | 29               |
| 28     | 40               |
| 33     | null             |
| 38     | null             |

## 核心查询逻辑

在`GroupByFillDataSet`中维护了两个主要变量

```
// the first value for each time series
private Object[] previousValue;
// last timestamp for each time series
private long[] lastTimeArray;
```
### `previousValue`

`previousValue`这个变量维护了当前时间窗口的前一个降采样值，在`GroupByFillDataSet`构造函数中调用了`initPreviousParis`方法对其进行初始化。

```
  private void initPreviousParis(QueryContext context, GroupByFillPlan groupByFillPlan)
          throws StorageEngineException, IOException, QueryProcessException {
    previousValue = new Object[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      Path path = paths.get(i);
      TSDataType dataType = dataTypes.get(i);
      IFill fill = new PreviousFill(dataType, groupByEngineDataSet.getStartTime(), -1L);
      fill.constructReaders(path, groupByFillPlan.getAllMeasurementsInDevice(path.getDevice()), context);

      TimeValuePair timeValuePair = fill.getFillResult();
      if (timeValuePair == null || timeValuePair.getValue() == null) {
        previousValue[i] = null;
      } else {
        previousValue[i] = timeValuePair.getValue().getValue();
      }
    }
  }
```

`initPreviousParis`方法主要为每个时间序列构造了一个单点补空值查询，`queryTime`设置为降采样时间窗口的起始值，`beforeRange`不作限制。

### `lastTimeArray`

`lastTimeArray`这个变量维护了每个时间序列的最近时间戳值，主要用于`PREVIOUSUNTILLAST`这一填充方式，在`GroupByFillDataSet`构造函数中调用了`initLastTimeArray`方法对其进行初始化。

```
  private void initLastTimeArray(QueryContext context)
      throws IOException, StorageEngineException, QueryProcessException {
    lastTimeArray = new long[paths.size()];
    Arrays.fill(lastTimeArray, Long.MAX_VALUE);
    for (int i = 0; i < paths.size(); i++) {
      TimeValuePair lastTimeValuePair =
          LastQueryExecutor.calculateLastPairForOneSeries(paths.get(i), dataTypes.get(i), context);
      if (lastTimeValuePair.getValue() != null) {
        lastTimeArray[i] = lastTimeValuePair.getTimestamp();
      }
    }
  }
```
`initPreviousParis`方法主要为每个时间序列构造了一个最近时间戳 Last 查询

### 填充过程

填充过程在`nextWithoutConstraint`方法中完成，主要逻辑如下：

```
protected RowRecord nextWithoutConstraint() throws IOException {

    // 首先通过groupByEngineDataSet，获得原始的降采样查询结果行
    RowRecord rowRecord = groupByEngineDataSet.nextWithoutConstraint();

    // 接下来对每个时间序列判断需不需要填充
    for (int i = 0; i < paths.size(); i++) {
      Field field = rowRecord.getFields().get(i);
      // 当前值为null，需要进行填充
      if (field.getDataType() == null) {
        // 当前一个值不为null 并且 (填充方式不是PREVIOUSUNTILLAST 或者 当前时间小于改时间序列的最近时间戳)
        if (previousValue[i] != null
            && (!((PreviousFill) fillTypes.get(dataTypes.get(i))).isUntilLast()
            || rowRecord.getTimestamp() <= lastTimeArray[i])) {
          rowRecord.getFields().set(i, Field.getField(previousValue[i], dataTypes.get(i)));
        }
      } else {
        // 当前值不为null，不需要填充，用当前值更新previousValue数组
        previousValue[i] = field.getObjectValue(field.getDataType());
      }
    }
    return rowRecord;
  }
```




