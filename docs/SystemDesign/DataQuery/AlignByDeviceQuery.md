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

# Query by device alignment

The table structure of AlignByDevice Query is:

| time | device | sensor1 | sensor2 | sensor3 | ...  |
| ---- | ------ | ------- | ------- | ------- | ---- |
|      |        |         |         |         |      |

## Design principle

The implementation principle of the device-by-device query is mainly to calculate the measurement points and filter conditions corresponding to each device in the query, and then the query is performed separately for each device, and the result set is assembled and returned.

### Meaning of important fields in AlignByDevicePlan

First explain the meaning of some important fields in AlignByDevicePlan:
- `List<String> measurements`：The list of measurements that appear in the query.
- `List<String> devices`: The list of devices got from prefix paths.
- `Map<String, IExpression> deviceToFilterMap`: This field is used to store the filter conditions corresponding to the device.
- `Map<String, TSDataType> measurementDataTypeMap`：This field is used to record the actual data type of the time series for the actual query, and its key value does not contain the aggregate functions.
- `Map<String, TSDataType> columnDataTypeMap`：This field is used to record the data type of each column in the result set. It's aim is to construct the header and output the result set, whose key value can contain aggregation functions.
- `enum MeasurementType`：Record measurement types. Measurements that do not exist in any device are of type `NonExist`; measurements that exist are of type `Exist`.
- `Map<String, MeasurementType> measurementTypeMap`: This field is used to record all measurement types in the query.
- groupByTimePlan, fillQueryPlan, aggregationPlan：To avoid redundancy, these three execution plans are set as subclasses of RawDataQueryPlan and set as variables in AlignByDevicePlan.  If the query plan belongs to one of these three plans, the field is assigned and saved.

Before explaining the specific implementation process, a relatively complete example is given first, and the following explanation will be used in conjunction with this example.

```sql
SELECT s1, *, s2, s5 FROM root.sg.d1, root.sg.* WHERE time = 1 AND s1 < 25 ALIGN BY DEVICE
```

Among them, the time series in the system is:

- root.sg.d1.s1
- root.sg.d1.s2
- root.sg.d2.s1

The storage group `root.sg` contains two devices d1 and d2, where d1 has two sensors s1 and s2, d2 has only sensor s1, and the same sensor s1 has the same data type.

The following will be explained according to the specific process:

### Logical plan generation

- org.apache.iotdb.db.qp.Planner

Unlike the original data query, the alignment by device query does not concatenate the suffix paths in the SELECT statement and the WHERE statement at this stage, but when the physical plan is subsequently generated, the mapping value and filter conditions corresponding to each device are calculated.

Therefore, the work done at this stage by device alignment only includes optimization of filter conditions in WHERE statements.

The optimization of the filtering conditions mainly includes three parts: removing the negation, transforming the disjunction paradigm, and merging the same path filtering conditions.  The corresponding optimizers are: RemoveNotOptimizer, DnfFilterOptimizer, MergeSingleFilterOptimizer.  This part of the logic can refer to:[Planner](../QueryEngine/Planner.md).

### Physical plan generation

- org.apache.iotdb.db.qp.strategy.PhysicalGenerator

After the logical plan is generated, the `transformToPhysicalPlan ()` method in the PhysicalGenerator class is called to convert the logical plan into a physical plan.  For device-aligned queries, the main logic of this method is implemented in the transformQuery () method.

**The main work done at this stage is to generate the corresponding** `AlignByDevicePlan`，**Fill in the variable information。**

It splices the suffix paths obtained in the SELECT statement with the prefix paths in the FROM clause to calculate the measurements of the query including its type and data type. The calculation process is as follows:

```java
  // Traversal suffix path
  for (int i = 0; i < suffixPaths.size(); i++) {
    Path suffixPath = suffixPaths.get(i);
    // Used to record the measurements corresponding to a suffix path.
    // See the following for an example
    Set<String> measurementSetOfGivenSuffix = new LinkedHashSet<>();

    // If not constant, it will be spliced with each device to get a complete path
    for (String device : devices) {
      Path fullPath = Path.addPrefixPath(suffixPath, device);
      try {
        // Wildcard has been removed from the device list, but suffix paths may still contain it
        // Get the actual time series paths by removing wildcards
        List<String> actualPaths = getMatchedTimeseries(fullPath.getFullPath());
        // If the path after splicing does not exist, it will be recognized as `NonExist` temporarily
        // If the measurement exists in next devices, then override `NonExist` to `Exist`
        if (actualPaths.isEmpty() && originAggregations.isEmpty()) {
          ...
        }

        // Get data types with and without aggregate functions (actual time series) respectively
        // Data type with aggregation function `columnDataTypes` is used for:
        //  1. Data type consistency check 2. Header calculation, output result set
        // The actual data type of the time series `measurementDataTypes` is used for
        //  the actual query in the AlignByDeviceDataSet
        String aggregation =
            originAggregations != null && !originAggregations.isEmpty()
                ? originAggregations.get(i) : null;
        Pair<List<TSDataType>, List<TSDataType>> pair = getSeriesTypes(actualPaths,
            aggregation);
        List<TSDataType> columnDataTypes = pair.left;
        List<TSDataType> measurementDataTypes = pair.right;

        for (int pathIdx = 0; pathIdx < actualPaths.size(); pathIdx++) {
          Path path = new Path(actualPaths.get(pathIdx));
          // Check the data type consistency of the sensors with the same name
          String measurementChecked;
          ...
          TSDataType columnDataType = columnDataTypes.get(pathIdx);
          // Check data type if there is a sensor with the same name
          if (columnDataTypeMap.containsKey(measurementChecked)) {
            // The data types is inconsistent, an exception will be thrown. End
            if (!columnDataType.equals(columnDataTypeMap.get(measurementChecked))) {
              throw new QueryProcessException(...);
            }
          } else {
            // There is no such measurement, it will be recorded
            ...
          }

          // This step indicates that the measurement exists under the device and is correct,
          // First, update measurementSetOfGivenSuffix which is distinct
          // Then if this measurement is recognized as NonExist before，update it to Exist
          if (measurementSetOfGivenSuffix.add(measurementChecked)
              || measurementTypeMap.get(measurementChecked) != MeasurementType.Exist) {
            measurementTypeMap.put(measurementChecked, MeasurementType.Exist);
          }
        }
          // update paths
          paths.add(path);
      } catch (MetadataException e) {
        throw new LogicalOptimizeException(...);
      }
    }
    // update measurements
    // Note that within a suffix path loop, SET is used to avoid duplicate measurements
    // While a LIST is used outside the loop to ensure that the output contains all measurements entered by the user
    // In the example，for suffix *, measurementSetOfGivenSuffix = {s1,s2}
    // for suffix s1, measurementSetOfGivenSuffix = {s1}
    // therefore the final measurements is [s1,s2,s1].
    measurements.addAll(measurementSetOfGivenSuffix);
  }
```

```java
Map<String, IExpression> concatFilterByDevice(List<String> devices,
      FilterOperator operator)
Input：Deduplicated devices list and un-stitched FilterOperator
Input：The deviceToFilterMap after splicing records the Filter information corresponding to each device
```

The `concatfilterbydevice()` method splices the filter conditions according to the devices to get the corresponding filter conditions of each device. The main processing logic of it is in `concatFilterPath ()`:

The `concatFilterPath ()` method traverses the unspliced FilterOperator binary tree to determine whether the node is a leaf node. If so, the path of the leaf node is taken. If the path starts with time or root, it is not processed, otherwise the device name and node are not processed.  The paths are spliced and returned; if not, all children of the node are iteratively processed.

In the example, the result of splicing the filter conditions of device 1 is `time = 1 AND root.sg.d1.s1 <25`, and device 2 is` time = 1 AND root.sg.d2.s1 <25`.

The following example summarizes the variable information calculated through this stage:

- measurement list `measurements`：`[s1, '1', s1, s2, s2, s5]`
- measurement type `measurementTypeMap`：
  -  `s1 -> Exist`
  -  `s2 -> Exist`
  -  `s5 -> NonExist`
- Filter condition `deviceToFilterMap` for each device:
  -  `root.sg.d1 -> time = 1 AND root.sg.d1.s1 < 25`
  -  `root.sg.d2 -> time = 1 AND root.sg.d2.s1 < 25`

### Constructing a Header (ColumnHeader)

- org.apache.iotdb.db.service.TSServiceImpl

After generating the physical plan, you can execute the executeQueryStatement () method in TSServiceImpl to generate a result set and return it. The first step is to construct the header.

Query by device alignment After calling the TSServiceImpl.getQueryColumnHeaders () method, enter TSServiceImpl.getAlignByDeviceQueryHeaders () according to the query type to construct the headers.

The `getAlignByDeviceQueryHeaders ()` method is declared as follows:

```java
private void getAlignByDeviceQueryHeaders(
      AlignByDevicePlan plan, List<String> respColumns, List<String> columnTypes)
Input：The currently executing physical plan AlignByDevicePlan and the column names that need to be output respColumns and their corresponding data types columnTypes
Input：Calculated column name respColumns and data type columnTypes
```

The specific implementation logic is as follows:

1. First add the `Device` column, whose data type is` TEXT`;
2. Traverse the list of measurements without deduplication to determine the type of measurement currently traversed. If it is an `Exist` type, get its type from the `columnDataTypeMap`; set the other two types to `TEXT`, and then add measurement and its type to the header data structure.
3. Deduplicate measurements based on the intermediate variable deduplicatedMeasurements.

The resulting header is:

| Time | Device | s1  | 1   | s1  | s2  | s2  | s5  |
| ---- | ------ | --- | --- | --- | --- | --- | --- |
|      |        |     |     |     |     |     |     |

The deduplicated `measurements` are `[s1, '1', s2, s5]`.

### Result set generation

After the ColumnHeader is generated, the final step is to populate the result set with the results and return.

#### Result set creation

- org.apache.iotdb.db.service.TSServiceImpl

At this stage, you need to call `TSServiceImpl.createQueryDataSet ()` to create a new result set. This part of the implementation logic is relatively simple. For AlignByDeviceQuery, you only need to create a new `AlignByDeviceDataSet`. In the constructor, the parameters in AlignByDevicePlan  Assign to the newly created result set.

#### Result set population

- org.apache.iotdb.db.utils.QueryDataSetUtils

Next you need to fill the results. AlignByDeviceQuery will call the `TSServiceImpl.fillRpcReturnData ()` method, and then enter the `QueryDataSetUtils.convertQueryDataSetByFetchSize ()` method according to the query type.

The important method for getting results in the `convertQueryDataSetByFetchSize ()` method is the `hasNext ()` method of QueryDataSet.

The main logic of the `hasNext ()` method is as follows:

1. Determine if there is a specified row offset `rowOffset`, if there is, skip the number of rows that need to be offset; if the total number of results is less than the specified offset, return false.
2. Determines whether there is a specified limit on the number of rows `rowLimit`, if there is, it compares the current number of output rows, and returns false if the current number of output rows is greater than the limit.
3. Enter `AlignByDeviceDataSet.hasNextWithoutConstraint ()` method

<br>

- org.apache.iotdb.db.query.dataset.AlignByDeviceDataSet

First explain the meaning of the important fields in the result set:

- `deviceIterator`: Query by device is essentially to calculate the mapping value and filtering conditions corresponding to each device, and then the query is performed separately for each device. This field is an iterator for the device. Each query obtains a device to perform.
- `currentDataSet`：This field represents the result set obtained by querying a certain device.

The work done by the `hasNextWithoutConstraint ()` method is mainly to determine whether the current result set has the next result, if not, the next device is obtained, the path, data type and filter conditions required by the device to execute the query are calculated, and then executed according to its query type  The result set is obtained after a specific query plan, until no device is available for querying.

The specific implementation logic is as follows:

1. First determine whether the current result set is initialized and there is a next result. If it is, it returns true directly, that is, you can call the `next()` method to get the next `RowRecord`; otherwise, the result set is not initialized and proceeds to step 2.
2. Iterate `deviceIterator` to get the devices needed for this execution, and then find the device node from MManger by the device path to get all sensor nodes under it.
3. Compare all measurements in the query and the sensor nodes under the current device to get the `executeColumns` which need to be queried. Then concatenate the current device name and measurements to calculate the query path, data type, and filter conditions of the current device. The corresponding fields are `executePaths`,` tsDataTypes`, and `expression`. If it is an aggregate query, you need to calculate `executeAggregations`.
4. Determine whether the current subquery type is GroupByQuery, AggregationQuery, FillQuery or RawDataQuery. Perform the corresponding query and return the result set. The implementation logic [Raw data query](../DataQuery/RawDataQuery.md)，[Aggregate query](../DataQuery/AggregationQuery.md)，[Downsampling query](../DataQuery/GroupByQuery.md) can be referenced.

After initializing the result set through the `hasNextWithoutConstraint ()` method and ensuring that there is a next result, you can call `QueryDataSet.next ()` method to get the next `RowRecord`.

The `next ()` method is mainly implemented as the `AlignByDeviceDataSet.nextWithoutConstraint ()` method.

The work done by the `nextWithoutConstraint ()` method is to ** transform the time-aligned result set form obtained by a single device query into a device-aligned result set form **, and return the transformed `RowRecord`.

The specific implementation logic is as follows:

1. First get the next time-aligned `originRowRecord` from the result set.
2. Create a new `RowRecord` with timestamp, add device columns to it, and first create a Map structure` currentColumnMap` of `measurementName-> Field` according to` executeColumns` and the obtained result.
3. After that, you only need to traverse the deduplicated `measurements` list to determine its type. If the type is` Exist`, get the corresponding result from the `currentColumnMap` according to the measurementName. If not, set it to` null`; if it is `NonExist  `Type is set to` null` directly.

After writing the output data stream according to the transformed `RowRecord`, the result set can be returned.
