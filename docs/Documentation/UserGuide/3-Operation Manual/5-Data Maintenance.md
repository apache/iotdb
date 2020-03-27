# Data Maintenance

## Data Update

Users can use [UPDATE statements](/document/V0.8.x/UserGuide/5-IoTDB%20SQL%20Documentation/1-IoTDB%20Query%20Statement.html) to update data over a period of time in a specified timeseries. When updating data, users can select a timeseries to be updated (version 0.8.2 does not support multiple timeseries updates) and specify a time point or period to be updated (version 0.8.0 must have time filtering conditions).

In a JAVA programming environment, you can use the [Java JDBC](/document/V0.8.x/UserGuide/6-JDBC%20API/1-JDBC%20API.html) to execute single or batch UPDATE statements.

### Update Single Timeseries

Taking the power supply status of ln group wf02 plant wt02 device as an example, there exists such a usage scenario:

After data access and analysis, it is found that the power supply status from 2017-11-01 15:54:00 to 2017-11-01 16:00:00 is true, but the actual power supply status is abnormal. You need to update the status to false during this period. The SQL statement for this operation is:

```
update root.ln.wf02 SET wt02.status = false where time <=2017-11-01T16:00:00 and time >= 2017-11-01T15:54:00
```
It should be noted that when the updated data type does not match the actual data type, IoTDB will give the corresponding error prompt as shown below:

```
IoTDB> update root.ln.wf02 set wt02.status = 1205 where time < now()
error: The BOOLEAN data type should be true/TRUE or false/FALSE
```
When the updated path does not exist, IoTDB will give the corresponding error prompt as shown below:

```
IoTDB> update root.ln.wf02 set wt02.sta = false where time < now()
Msg: do not select any existing series
```
## Data Deletion

Users can delete data that meet the deletion condition in the specified timeseries by using the [DELETE statement](/document/V0.8.x/UserGuide/5-IoTDB%20SQL%20Documentation/1-IoTDB%20Query%20Statement.html). When deleting data, users can select one or more timeseries paths, prefix paths, or paths with star  to delete data before a certain time (version 0.8.2 does not support the deletion of data within a closed time interval).

In a JAVA programming environment, you can use the [Java JDBC](/document/V0.8.x/UserGuide/6-JDBC%20API/1-JDBC%20API.html) to execute single or batch UPDATE statements.

### Delete Single Timeseries

Taking ln Group as an example, there exists such a usage scenario:

The wf02 plant's wt02 device has many segments of errors in its power supply status before 2017-11-01 16:26:00, and the data cannot be analyzed correctly. The erroneous data affected the correlation analysis with other devices. At this point, the data before this time point needs to be deleted. The SQL statement for this operation is

```
delete from root.ln.wf02.wt02.status where time<=2017-11-01T16:26:00;
```

### Delete Multiple Timeseries

When both the power supply status and hardware version of the ln group wf02 plant wt02 device before 2017-11-01 16:26:00 need to be deleted, [the prefix path with broader meaning or the path with star](/document/V0.8.x/UserGuide/2-Concept%20Key%20Concepts%20and%20Terminology/1-Key%20Concepts%20and%20Terminology.html) can be used to delete the data. The SQL statement for this operation is:

```
delete from root.ln.wf02.wt02 where time <= 2017-11-01T16:26:00;
```
or

```
delete from root.ln.wf02.wt02.* where time <= 2017-11-01T16:26:00;
```
It should be noted that when the deleted path does not exist, IoTDB will give the corresponding error prompt as shown below:

```
IoTDB> delete from root.ln.wf03.wt02.status where time < now()
Msg: TimeSeries does not exist and its data cannot be deleted
```
