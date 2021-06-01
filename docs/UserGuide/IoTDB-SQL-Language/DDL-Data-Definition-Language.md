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

# IoTDB-SQL Language

## Data Definition Language (DDL) 

### Stroage Group Management
#### Create Storage Group

According to the storage model we can set up the corresponding storage group. The SQL statements for creating storage groups are as follows:

```
IoTDB > set storage group to root.ln
IoTDB > set storage group to root.sgcc
```

We can thus create two storage groups using the above two SQL statements.

It is worth noting that when the path itself or the parent/child layer of the path is already set as a storage group, the path is then not allowed to be set as a storage group. For example, it is not feasible to set `root.ln.wf01` as a storage group when two storage groups `root.ln` and `root.sgcc` exist. The system gives the corresponding error prompt as shown below:

```
IoTDB> set storage group to root.ln.wf01
Msg: 300: root.ln has already been set to storage group.
```
The LayerName of storage group can only be characters, numbers, underscores and hyphens. 
 
Besides, if deploy on Windows system, the LayerName is case-insensitive, which means it's not allowed to set storage groups `root.ln` and `root.LN` at the same time.

#### Show Storage Group

After creating the storage group, we can use the [SHOW STORAGE GROUP](../Appendix/SQL-Reference.md) statement and [SHOW STORAGE GROUP \<PrefixPath>](../Appendix/SQL-Reference.md) to view the storage groups. The SQL statements are as follows:

```
IoTDB> show storage group
IoTDB> show storage group root.ln
```

The result is as follows:

```
+-------------+
|storage group|
+-------------+
|    root.sgcc|
|      root.ln|
+-------------+
Total line number = 2
It costs 0.060s
```

#### Delete Storage Group

User can use the `DELETE STORAGE GROUP <PrefixPath>` statement to delete all storage groups under the prefixPath. Please note the data in the storage group will also be deleted. 

```
IoTDB > DELETE STORAGE GROUP root.ln
IoTDB > DELETE STORAGE GROUP root.sgcc
// delete all data, all timeseries and all storage groups
IoTDB > DELETE STORAGE GROUP root.*
```
### Timeseries Management
#### Create Timeseries

According to the storage model selected before, we can create corresponding timeseries in the two storage groups respectively. The SQL statements for creating timeseries are as follows:

```
IoTDB > create timeseries root.ln.wf01.wt01.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.ln.wf01.wt01.temperature with datatype=FLOAT,encoding=RLE
IoTDB > create timeseries root.ln.wf02.wt02.hardware with datatype=TEXT,encoding=PLAIN
IoTDB > create timeseries root.ln.wf02.wt02.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.sgcc.wf03.wt01.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.sgcc.wf03.wt01.temperature with datatype=FLOAT,encoding=RLE
```

We could also create **aligned** timeseries:

```
IoTDB > create aligned timeseries root.sg.d1.(s1 FLOAT, s2 INT32)
IoTDB > create aligned timeseries root.sg.d1.(s3 FLOAT, s4 INT32) with encoding=(RLE, Grollia), compression=SNAPPY
```

Attention: Aligned timeseries must have the same compression type.

Notice that when in the CREATE TIMESERIES statement the encoding method conflicts with the data type, the system gives the corresponding error prompt as shown below:

```
IoTDB > create timeseries root.ln.wf02.wt02.status WITH DATATYPE=BOOLEAN, ENCODING=TS_2DIFF
error: encoding TS_2DIFF does not support BOOLEAN
```

Please refer to [Encoding](../Data-Concept/Encoding.md) for correspondence between data type and encoding.

### Create and set device template
```

IoTDB > set storage group root.beijing

// create a device templat
IoTDB > create device template temp1(
  (s1 INT32 with encoding=Gorilla, compression=SNAPPY),
  (s2 FLOAT with encoding=RLE, compression=SNAPPY)
 )

// set device template to storage group "root.beijing"
IoTDB > set device template temp1 to root.beijing

```

#### Delete Timeseries

To delete the timeseries we created before, we are able to use `DELETE TimeSeries <PrefixPath>` statement.

The usage are as follows:

```
IoTDB> delete timeseries root.ln.wf01.wt01.status
IoTDB> delete timeseries root.ln.wf01.wt01.temperature, root.ln.wf02.wt02.hardware
IoTDB> delete timeseries root.ln.wf02.*
```

As for **aligned** timeseries, we could delete them by explicit declaration with parentheses.

```
IoTDB > delete timeseries root.sg.d1.(s1,s2)
```

Attention: Deleting part of aligned timeseries is **not supported** currently.

```
IoTDB > delete timeseries root.sg.d1.s1
error: Not support deleting part of aligned timeseies!
```

#### Show Timeseries

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

```
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+
|                     timeseries|   alias|storage group|dataType|encoding|compression|                                       tags|                                              attributes|
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+
|root.sgcc.wf03.wt01.temperature|    null|    root.sgcc|   FLOAT|     RLE|     SNAPPY|                                       null|                                                    null|
|     root.sgcc.wf03.wt01.status|    null|    root.sgcc| BOOLEAN|   PLAIN|     SNAPPY|                                       null|                                                    null|
|             root.turbine.d1.s1|newAlias| root.turbine|   FLOAT|     RLE|     SNAPPY|{"newTag1":"newV1","tag4":"v4","tag3":"v3"}|{"attr2":"v2","attr1":"newV1","attr4":"v4","attr3":"v3"}|
|     root.ln.wf02.wt02.hardware|    null|      root.ln|    TEXT|   PLAIN|     SNAPPY|                                       null|                                                    null|
|       root.ln.wf02.wt02.status|    null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|                                       null|                                                    null|
|  root.ln.wf01.wt01.temperature|    null|      root.ln|   FLOAT|     RLE|     SNAPPY|                                       null|                                                    null|
|       root.ln.wf01.wt01.status|    null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|                                       null|                                                    null|
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+
Total line number = 7
It costs 0.016s

+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                   timeseries|alias|storage group|dataType|encoding|compression|tags|attributes|
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|   root.ln.wf02.wt02.hardware| null|      root.ln|    TEXT|   PLAIN|     SNAPPY|null|      null|
|     root.ln.wf02.wt02.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
|root.ln.wf01.wt01.temperature| null|      root.ln|   FLOAT|     RLE|     SNAPPY|null|      null|
|     root.ln.wf01.wt01.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
Total line number = 4
It costs 0.004s
```

* SHOW TIMESERIES (<`PrefixPath`>)? WhereClause
 
  returns all the timeseries information that satisfy the where condition and start with the prefixPath SQL statements are as follows:

```
ALTER timeseries root.ln.wf02.wt02.hardware ADD TAGS unit=c
ALTER timeseries root.ln.wf02.wt02.status ADD TAGS description=test1
show timeseries root.ln where unit=c
show timeseries root.ln where description contains 'test1'
```

The results are shown below respectly:

```
+--------------------------+-----+-------------+--------+--------+-----------+------------+----------+
|                timeseries|alias|storage group|dataType|encoding|compression|        tags|attributes|
+--------------------------+-----+-------------+--------+--------+-----------+------------+----------+
|root.ln.wf02.wt02.hardware| null|      root.ln|    TEXT|   PLAIN|     SNAPPY|{"unit":"c"}|      null|
+--------------------------+-----+-------------+--------+--------+-----------+------------+----------+
Total line number = 1
It costs 0.005s

+------------------------+-----+-------------+--------+--------+-----------+-----------------------+----------+
|              timeseries|alias|storage group|dataType|encoding|compression|                   tags|attributes|
+------------------------+-----+-------------+--------+--------+-----------+-----------------------+----------+
|root.ln.wf02.wt02.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|{"description":"test1"}|      null|
+------------------------+-----+-------------+--------+--------+-----------+-----------------------+----------+
Total line number = 1
It costs 0.004s
```

> Notice that, we only support one condition in the where clause. Either it's an equal filter or it is an `contains` filter. In both case, the property in the where condition must be a tag.

* SHOW TIMESERIES LIMIT INT OFFSET INT

  returns all the timeseries information start from the offset and limit the number of series returned
  
* SHOW LATEST TIMESERIES

  all the returned timeseries information should be sorted in descending order of the last timestamp of timeseries
  
It is worth noting that when the queried path does not exist, the system will return no timeseries.  


#### Count Timeseries

IoTDB is able to use `COUNT TIMESERIES <Path>` to count the number of timeseries in the path. SQL statements are as follows:

```
IoTDB > COUNT TIMESERIES root
IoTDB > COUNT TIMESERIES root.ln
IoTDB > COUNT TIMESERIES root.ln.*.*.status
IoTDB > COUNT TIMESERIES root.ln.wf01.wt01.status
```

Besides, `LEVEL` could be defined to show count the number of timeseries of each node at the given level in current Metadata Tree. This could be used to query the number of sensors under each device. The grammar is: `COUNT TIMESERIES <Path> GROUP BY LEVEL=<INTEGER>`.

For example, if there are several timeseries (use `show timeseries` to show all timeseries):

```
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+
|                     timeseries|   alias|storage group|dataType|encoding|compression|                                       tags|                                              attributes|
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+
|root.sgcc.wf03.wt01.temperature|    null|    root.sgcc|   FLOAT|     RLE|     SNAPPY|                                       null|                                                    null|
|     root.sgcc.wf03.wt01.status|    null|    root.sgcc| BOOLEAN|   PLAIN|     SNAPPY|                                       null|                                                    null|
|             root.turbine.d1.s1|newAlias| root.turbine|   FLOAT|     RLE|     SNAPPY|{"newTag1":"newV1","tag4":"v4","tag3":"v3"}|{"attr2":"v2","attr1":"newV1","attr4":"v4","attr3":"v3"}|
|     root.ln.wf02.wt02.hardware|    null|      root.ln|    TEXT|   PLAIN|     SNAPPY|                               {"unit":"c"}|                                                    null|
|       root.ln.wf02.wt02.status|    null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|                    {"description":"test1"}|                                                    null|
|  root.ln.wf01.wt01.temperature|    null|      root.ln|   FLOAT|     RLE|     SNAPPY|                                       null|                                                    null|
|       root.ln.wf01.wt01.status|    null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|                                       null|                                                    null|
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+
Total line number = 7
It costs 0.004s
```

Then the Metadata Tree will be as below:

<center><img style="width:100%; max-width:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/69792176-1718f400-1201-11ea-861a-1a83c07ca144.jpg"></center>

As can be seen, `root` is considered as `LEVEL=0`. So when you enter statements such as:

```
IoTDB > COUNT TIMESERIES root GROUP BY LEVEL=1
IoTDB > COUNT TIMESERIES root.ln GROUP BY LEVEL=2
IoTDB > COUNT TIMESERIES root.ln.wf01 GROUP BY LEVEL=2
```

You will get following results:

```
+------------+-----+
|      column|count|
+------------+-----+
|   root.sgcc|    2|
|root.turbine|    1|
|     root.ln|    4|
+------------+-----+
Total line number = 3
It costs 0.002s

+------------+-----+
|      column|count|
+------------+-----+
|root.ln.wf02|    2|
|root.ln.wf01|    2|
+------------+-----+
Total line number = 2
It costs 0.002s

+------------+-----+
|      column|count|
+------------+-----+
|root.ln.wf01|    2|
+------------+-----+
Total line number = 1
It costs 0.002s
```

> Note: The path of timeseries is just a filter condition, which has no relationship with the definition of level.

#### Tag and attribute management

We can also add an alias, extra tag and attribute information while creating one timeseries.
The SQL statements for creating timeseries with extra tag and attribute information are extended as follows:

```
create timeseries root.turbine.d1.s1(temprature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2) attributes(attr1=v1, attr2=v2)
```

The `temprature` in the brackets is an alias for the sensor `s1`. So we can use `temprature` to replace `s1` anywhere.

> IoTDB also supports [using AS function](../Appendix/DML-Data-Manipulation-Language.md) to set alias. The difference between the two is: the alias set by the AS function is used to replace the whole time series name, temporary and not bound with the time series; while the alias mentioned above is only used as the alias of the sensor, which is bound with it and can be used equivalent to the original sensor name.

The only difference between tag and attribute is that we will maintain an inverted index on the tag, so we can use tag property in the show timeseries where clause which you can see in the following `Show Timeseries` section.

> Notice that the size of the extra tag and attribute information shouldn't exceed the `tag_attribute_total_size`.

We can update the tag information after creating it as following:

* Rename the tag/attribute key
```
ALTER timeseries root.turbine.d1.s1 RENAME tag1 TO newTag1
```
* reset the tag/attribute value
```
ALTER timeseries root.turbine.d1.s1 SET newTag1=newV1, attr1=newV1
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

### Node Management
#### Show Child Paths

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
Total line number = 2
It costs 0.002s
```

> get all paths in form of root.xx.xx.xx：show child paths root.xx.xx

#### Show Child Nodes

```
SHOW CHILD NODES prefixPath
```

Return all child nodes of the prefixPath.

Example：

* return the child nodes of root：show child nodes root

```
+------------+
| child nodes|
+------------+
|          ln|
+------------+
```

* return the child nodes of root.vehicle：show child nodes root.ln

```
+------------+
| child nodes|
+------------+
|        wf01|
|        wf02|
+------------+
```

#### Count Nodes

IoTDB is able to use `COUNT NODES <PrefixPath> LEVEL=<INTEGER>` to count the number of nodes at the given level in current Metadata Tree. This could be used to query the number of devices. The usage are as follows:

```
IoTDB > COUNT NODES root LEVEL=2
IoTDB > COUNT NODES root.ln LEVEL=2
IoTDB > COUNT NODES root.ln.wf01 LEVEL=3
```

As for the above mentioned example and Metadata tree, you can get following results:

```
+-----+
|count|
+-----+
|    4|
+-----+
Total line number = 1
It costs 0.003s

+-----+
|count|
+-----+
|    2|
+-----+
Total line number = 1
It costs 0.002s

+-----+
|count|
+-----+
|    1|
+-----+
Total line number = 1
It costs 0.002s
```

> Note: The path of timeseries is just a filter condition, which has no relationship with the definition of level.
`PrefixPath` could contains `*`, but all nodes after `*` would be ignored. Only the prefix path before `*` is valid.

#### Show Devices

* SHOW DEVICES prefixPath? (WITH STORAGE GROUP)? limitClause? #showDevices

Similar to `Show Timeseries`, IoTDB also supports two ways of viewing devices:

* `SHOW DEVICES` statement presents all devices' information, which is equal to `SHOW DEVICES root`.
* `SHOW DEVICES <PrefixPath>` statement specifies the `PrefixPath` and returns the devices information under the given level.

SQL statement is as follows:

```
IoTDB> show devices
IoTDB> show devices root.ln
```

You can get results below:

```
+-------------------+
|            devices|
+-------------------+
|  root.ln.wf01.wt01|
|  root.ln.wf02.wt02|
|root.sgcc.wf03.wt01|
|    root.turbine.d1|
+-------------------+
Total line number = 4
It costs 0.002s

+-----------------+
|          devices|
+-----------------+
|root.ln.wf01.wt01|
|root.ln.wf02.wt02|
+-----------------+
Total line number = 2
It costs 0.001s
```

To view devices' information with storage group, we can use `SHOW DEVICES WITH STORAGE GROUP` statement.

* `SHOW DEVICES WITH STORAGE GROUP` statement presents all devices' information with their storage group.
* `SHOW DEVICES <PrefixPath> WITH STORAGE GROUP` statement specifies the `PrefixPath` and returns the 
devices' information under the given level with their storage group information.

SQL statement is as follows:

```
IoTDB> show devices with storage group
IoTDB> show devices root.ln with storage group
```

You can get results below:

```
+-------------------+-------------+
|            devices|storage group|
+-------------------+-------------+
|  root.ln.wf01.wt01|      root.ln|
|  root.ln.wf02.wt02|      root.ln|
|root.sgcc.wf03.wt01|    root.sgcc|
|    root.turbine.d1| root.turbine|
+-------------------+-------------+
Total line number = 4
It costs 0.003s

+-----------------+-------------+
|          devices|storage group|
+-----------------+-------------+
|root.ln.wf01.wt01|      root.ln|
|root.ln.wf02.wt02|      root.ln|
+-----------------+-------------+
Total line number = 2
It costs 0.001s
```

### TTL

IoTDB supports storage-level TTL settings, which means it is able to delete old data automatically and periodically. The benefit of using TTL is that hopefully you can control the total disk space usage and prevent the machine from running out of disks. Moreover, the query performance may downgrade as the total number of files goes up and the memory usage also increase as there are more files. Timely removing such files helps to keep at a high query performance level and reduce memory usage.

#### Set TTL

The SQL Statement for setting TTL is as follow:

```
IoTDB> set ttl to root.ln 3600000
```

This example means that for data in `root.ln`, only that of the latest 1 hour will remain, the older one is removed or made invisible.

#### Unset TTL

To unset TTL, we can use follwing SQL statement:

```
IoTDB> unset ttl to root.ln
```

After unset TTL, all data will be accepted in `root.ln`

#### Show TTL

To Show TTL, we can use following SQL statement:

```
IoTDB> SHOW ALL TTL
IoTDB> SHOW TTL ON StorageGroupNames
```

The SHOW ALL TTL example gives the TTL for all storage groups.
The SHOW TTL ON  root.group1 , root.group2 , root.group3 example shows the TTL for the three storage 
groups specified.
Note: the TTL for storage groups that do not have a TTL set will display as null.

