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

# DDL (Data Definition Language)

## Create Storage Group

According to the storage model we can set up the corresponding storage group. The SQL statements for creating storage groups are as follows:

```
IoTDB > set storage group to root.ln
IoTDB > set storage group to root.sgcc
```

We can thus create two storage groups using the above two SQL statements.

It is worth noting that when the path itself or the parent/child layer of the path is already set as a storage group, the path is then not allowed to be set as a storage group. For example, it is not feasible to set `root.ln.wf01` as a storage group when two storage groups `root.ln` and `root.sgcc` exist. The system gives the corresponding error prompt as shown below:

```
IoTDB> set storage group to root.ln.wf01
Msg: org.apache.iotdb.exception.MetadataException: org.apache.iotdb.exception.MetadataException: The prefix of root.ln.wf01 has been set to the storage group.
```

## Show Storage Group

After the storage group is created, we can use the [SHOW STORAGE GROUP](../Operation%20Manual/SQL%20Reference.md) statement and [SHOW STORAGE GROUP \<PrefixPath>](../Operation%20Manual/SQL%20Reference.md) to view the storage groups. The SQL statements are as follows:

```
IoTDB> show storage group
IoTDB> show storage group root.ln
```

The result is as follows:
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/67779101/92545299-6c029400-f282-11ea-80ea-b672a57f4b13.png"></center>

## Delete Storage Group

User can use the `DELETE STORAGE GROUP <PrefixPath>` statement to delete all storage groups under the prefixPath. Please note the data in the storage group will also be deleted. 

```
IoTDB > DELETE STORAGE GROUP root.ln
IoTDB > DELETE STORAGE GROUP root.sgcc
// delete all data, all timeseries and all storage groups
IoTDB > DELETE STORAGE GROUP root.*
```

## Create Timeseries

According to the storage model selected before, we can create corresponding timeseries in the two storage groups respectively. The SQL statements for creating timeseries are as follows:

```
IoTDB > create timeseries root.ln.wf01.wt01.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.ln.wf01.wt01.temperature with datatype=FLOAT,encoding=RLE
IoTDB > create timeseries root.ln.wf02.wt02.hardware with datatype=TEXT,encoding=PLAIN
IoTDB > create timeseries root.ln.wf02.wt02.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.sgcc.wf03.wt01.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.sgcc.wf03.wt01.temperature with datatype=FLOAT,encoding=RLE
```

Notice that when in the CRATE TIMESERIES statement the encoding method conflicts with the data type, the system gives the corresponding error prompt as shown below:

```
IoTDB> create timeseries root.ln.wf02.wt02.status WITH DATATYPE=BOOLEAN, ENCODING=TS_2DIFF
error: encoding TS_2DIFF does not support BOOLEAN
```

Please refer to [Encoding](../Concept/Encoding.md) for correspondence between data type and encoding.

### Tag and attribute management

We can also add an alias, extra tag and attribute information while creating one timeseries.
The SQL statements for creating timeseries with extra tag and attribute information are extended as follows:

```
create timeseries root.turbine.d1.s1(temprature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2) attributes(attr1=v1, attr2=v2)
```

The `temprature` in the brackets is an alias for the sensor `s1`. So we can use `temprature` to replace `s1` anywhere.

> IoTDB also supports [using AS function](../Operation%20Manual/DML%20Data%20Manipulation%20Language.md) to set alias. The difference between the two is: the alias set by the AS function is used to replace the whole time series name, temporary and not bound with the time series; while the alias mentioned above is only used as the alias of the sensor, which is bound with it and can be used equivalent to the original sensor name.

The only difference between tag and attribute is that we will maintain an inverted index on the tag, so we can use tag property in the show timeseries where clause which you can see in the following `Show Timeseries` section.

> Notice that the size of the extra tag and attribute information shouldn't exceed the `tag_attribute_total_size`.


## UPDATE TAG OPERATION
We can update the tag information after creating it as following:

* Rename the tag/attribute key
```
ALTER timeseries root.turbine.d1.s1 RENAME tag1 TO newTag1
```
* reset the tag/attribute value
```
ALTER timeseries root.turbine.d1.s1 SET tag1=newV1, attr1=newV1
```
* delete the existing tag/attribute
```
ALTER timeseries root.turbine.d1.s1 DROP tag1, tag2
```
* add new tags
```
ALTER timeseries root.turbine.d1.s1 ADD TAGS tag3=v3, tag4=v4
```
* add new attributes
```
ALTER timeseries root.turbine.d1.s1 ADD ATTRIBUTES attr3=v3, attr4=v4
```
* upsert alias, tags and attributes
> add alias or a new key-value if the alias or key doesn't exist, otherwise, update the old one with new value.
```
ALTER timeseries root.turbine.d1.s1 UPSERT ALIAS=newAlias TAGS(tag3=v3, tag4=v4) ATTRIBUTES(attr3=v3, attr4=v4)
```

## Show Timeseries

* SHOW LATEST? TIMESERIES prefixPath? showWhereClause? limitClause?

  There are four optional clauses added in SHOW TIMESERIES, return information of time series 
  
Timeseries information includes: timeseries path, alias of measurement, storage group it belongs to, data type, encoding type, compression type, tags and attributes.
 
Examples:

* SHOW TIMESERIES

  presents all timeseries information in JSON form
 
* SHOW TIMESERIES <`Path`> 
  
  returns all timeseries information under the given <`Path`>.  <`Path`> needs to be a prefix path or a path with star or a timeseries path. SQL statements are as follows:

```
IoTDB> show timeseries root
IoTDB> show timeseries root.ln
```

The results are shown below respectively:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577347-8db7d780-1ef4-11e9-91d6-764e58c10e94.jpg"></center>
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577359-97413f80-1ef4-11e9-8c10-53b291fc10a5.jpg"></center>

* SHOW TIMESERIES (<`PrefixPath`>)? WhereClause
 
  returns all the timeseries information that satisfy the where condition and start with the prefixPath SQL statements are as follows:

```
show timeseries root.ln where unit=c
show timeseries root.ln where description contains 'test1'
```

The results are shown below respectly:
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/16079446/79682385-61544d80-8254-11ea-8c23-9e93e7152fda.png"></center>

> Notice that, we only support one condition in the where clause. Either it's an equal filter or it is an `contains` filter. In both case, the property in the where condition must be a tag.

* SHOW TIMESERIES LIMIT INT OFFSET INT

  returns all the timeseries information start from the offset and limit the number of series returned
  
* SHOW LATEST TIMESERIES

  all the returned timeseries information should be sorted in descending order of the last timestamp of timeseries
  
It is worth noting that when the queried path does not exist, the system will return no timeseries.  

## Show Child Paths

```
SHOW CHILD PATHS prefixPath
```

Return all child paths of the prefixPath, the prefixPath could contains *.

Example：

* return the child paths of root.ln：show child paths root.ln

```
+------------+
| child paths|
+------------+
|root.ln.wf01|
|root.ln.wf02|
+------------+
```

* get all paths in form of root.xx.xx.xx：show child paths root.\*.\*

```
+---------------+
|    child paths|
+---------------+
|root.ln.wf01.s1|
|root.ln.wf02.s2|
+---------------+
```

## Count Timeseries

IoTDB is able to use `COUNT TIMESERIES <Path>` to count the number of timeseries in the path. SQL statements are as follows:

```
IoTDB > COUNT TIMESERIES root
IoTDB > COUNT TIMESERIES root.ln
IoTDB > COUNT TIMESERIES root.ln.*.*.status
IoTDB > COUNT TIMESERIES root.ln.wf01.wt01.status
```

Besides, `LEVEL` could be defined to show count the number of timeseries of each node at the given level in current Metadata Tree. This could be used to query the number of sensors under each device. The grammar is: `COUNT TIMESERIES <Path> GROUP BY LEVEL=<INTEGER>`.

For example, if there are several timeseries (use `show timeseries` to show all timeseries):
<center><img style="width:100%; max-width:800px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/69792072-cdc8a480-1200-11ea-8cec-321fef618a12.png"></center>

Then the Metadata Tree will be as below:
<center><img style="width:100%; max-width:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/69792176-1718f400-1201-11ea-861a-1a83c07ca144.jpg"></center>

As can be seen, `root` is considered as `LEVEL=0`. So when you enter statements such as:

```
IoTDB > COUNT TIMESERIES root GROUP BY LEVEL=1
IoTDB > COUNT TIMESERIES root.ln GROUP BY LEVEL=2
IoTDB > COUNT TIMESERIES root.ln.wf01 GROUP BY LEVEL=2
```

You will get following results:

<center><img style="width:100%; max-width:800px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/69792071-cb664a80-1200-11ea-8386-02dd12046c4b.png"></center>

> Note: The path of timeseries is just a filter condition, which has no relationship with the definition of level.

## Count Nodes

IoTDB is able to use `COUNT NODES <PrefixPath> LEVEL=<INTEGER>` to count the number of nodes at the given level in current Metadata Tree. This could be used to query the number of devices. The usage are as follows:

```
IoTDB > COUNT NODES root LEVEL=2
IoTDB > COUNT NODES root.ln LEVEL=2
IoTDB > COUNT NODES root.ln.wf01 LEVEL=3
```

As for the above mentioned example and Metadata tree, you can get following results:

<center><img style="width:100%; max-width:800px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/69792060-c73a2d00-1200-11ea-8ec4-be7145fd6c8c.png"></center>

> Note: The path of timeseries is just a filter condition, which has no relationship with the definition of level.
`PrefixPath` could contains `*`, but all nodes after `*` would be ignored. Only the prefix path before `*` is valid.

## Delete Timeseries

To delete the timeseries we created before, we are able to use `DELETE TimeSeries <PrefixPath>` statement.

The usage are as follows:

```
IoTDB> delete timeseries root.ln.wf01.wt01.status
IoTDB> delete timeseries root.ln.wf01.wt01.temperature, root.ln.wf02.wt02.hardware
IoTDB> delete timeseries root.ln.wf02.*
```

## Show Devices

Similar to `Show Timeseries`, IoTDB also supports two ways of viewing devices:

* `SHOW DEVICES` statement presents all devices information, which is equal to `SHOW DEVICES root`.
* `SHOW DEVICES <PrefixPath>` statement specifies the `PrefixPath` and returns the devices information under the given level.

SQL statement is as follows:

```
IoTDB> show devices
IoTDB> show devices root.ln
```

# TTL

IoTDB supports storage-level TTL settings, which means it is able to delete old data automatically and periodically. The benefit of using TTL is that hopefully you can control the total disk space usage and prevent the machine from running out of disks. Moreover, the query performance may downgrade as the total number of files goes up and the memory usage also increase as there are more files. Timely removing such files helps to keep at a high query performance level and reduce memory usage.

## Set TTL

The SQL Statement for setting TTL is as follow:

```
IoTDB> set ttl to root.ln 3600000
```

This example means that for data in `root.ln`, only that of the latest 1 hour will remain, the older one is removed or made invisible.

## Unset TTL

To unset TTL, we can use follwing SQL statement:

```
IoTDB> unset ttl to root.ln
```

After unset TTL, all data will be accepted in `root.ln`

## Show TTL

To Show TTL, we can use follwing SQL statement:

```
IoTDB> SHOW ALL TTL
IoTDB> SHOW TTL ON StorageGroupNames
```

The SHOW ALL TTL example gives the TTL for all storage groups.
The SHOW TTL ON  root.group1 , root.group2 , root.group3 example shows the TTL for the three storage 
groups specified.
Note: the TTL for storage groups that do not have a TTL set will display as null.

## FLUSH

Persist all the data points in the memory table of the storage group to the disk, and seal the data file.

```
IoTDB> FLUSH 
IoTDB> FLUSH root.ln
IoTDB> FLUSH root.sg1,root.sg2
```

## MERGE

Merge sequence and unsequence data. Currently IoTDB supports the following two types of SQL to manually trigger the merge process of data files:

* `MERGE` Only rewrite overlapped Chunks, the merge speed is quick, while there will be redundant data on the disk eventually.
* `FULL MERGE` Rewrite all data in overlapped files, the merge speed is slow, but there will be no redundant data on the disk eventually.

```
IoTDB> MERGE
IoTDB> FULL MERGE
```

## CLEAR CACHE

Clear the cache of chunk, chunk metadata and timeseries metadata to release the memory footprint.
```
IoTDB> CLEAR CACHE
```

## CREATE SNAPSHOT FOR SCHEMA

To speed up restarting of IoTDB, users can create snapshot of schema and avoid recovering schema from mlog file.
```
IoTDB> CREATE SNAPSHOT FOR SCHEMA
```
