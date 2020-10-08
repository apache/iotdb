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

# Group by fill

The main logic of GroupByFill query is in `GroupByFillDataSet`

* `org.apache.iotdb.db.query.dataset.groupby.GroupByFillDataSet`

GroupByFill is used to fill the null value of the group by result.

Note that:
* In group by fill, sliding step is not supported in group by clause
* Now, only last_value aggregation function is supported in group by fill.
* Linear fill is not supported in group by fill.

## Difference between PREVIOUSUNTILLAST fill and PREVIOUS fill

PREVIOUS will fill every interval generated from group by if possible, However, PREVIOUSUNTILLAST will just fill until the last time of the specified time series and the interval after the last time won't be filled and will be null.

Here is an example:

Timestamp of point A is 1, point B is 5, point C is 20, point D is 30, point N is 8 and point M is 38.

Raw Data is like: 
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/16079446/78784824-9f41ae00-79d8-11ea-9920-0825e081cae0.png">

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


When we use Previous fill, even though the data between D and M is from the future, we should also use value of point D to fill them.

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

However, When we use NONLASTPREVIOUS fill, because data between D and M is from the future, we won't fill them and still return null.

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

## Core query logic

We maintain two primary variable in `GroupByFillDataSet`

```
// the first value for each time series
private Object[] previousValue;
// last timestamp for each time series
private long[] lastTimeArray;
```
### `previousValue`

`previousValue` maintain the previous value before current time interval for each time series and we initialize it by calling the `initPreviousParis` method in the constructor of `GroupByFillDataSet`.

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

`initPreviousParis` construct a Single point supplementary null query for each time series and the parameter `queryTime` is set to the start time of group by query and the parameter `beforeRange` is set to -1.

### `lastTimeArray`

`lastTimeArray` maintain the last timestamp for each time series and is used in PREVIOUSUNTILLAST fill way. initialize it by calling the `initLastTimeArray` method in the constructor of `GroupByFillDataSet`.

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

`initPreviousParis` construct a last query for each time series.

### The process of filling

The logic of filling is in the `nextWithoutConstraint` method:

```
protected RowRecord nextWithoutConstraint() throws IOException {

    // get group by result without filling through groupByEngineDataSet
    RowRecord rowRecord = groupByEngineDataSet.nextWithoutConstraint();

    // judge whether each time series is needed to be filled
    for (int i = 0; i < paths.size(); i++) {
      Field field = rowRecord.getFields().get(i);
      // current group by result is null
      if (field.getDataType() == null) {
        // the previous value is not null and (fill type is not previous until last or now time is before last time)
        if (previousValue[i] != null
            && (!((PreviousFill) fillTypes.get(dataTypes.get(i))).isUntilLast()
            || rowRecord.getTimestamp() <= lastTimeArray[i])) {
          rowRecord.getFields().set(i, Field.getField(previousValue[i], dataTypes.get(i)));
        }
      } else {
        // current group by result is not null，no need to fill
        // use now value update previous value
        previousValue[i] = field.getObjectValue(field.getDataType());
      }
    }
    return rowRecord;
  }
```




