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

# Release version

<table>
	<tr>
      <th>Version</th>
	    <th colspan="3">IoTDB Binaries</th>
	    <th colspan="3">IoTDB Sources</th>
	    <th>release notes</th>  
	</tr>
	<tr>
            <td rowspan="2">1.0.1</td>
            <td><a href="https://www.apache.org/dyn/closer.cgi/iotdb/1.0.1/apache-iotdb-1.0.1-all-bin.zip">All-in-one</a></td>
            <td><a href="https://downloads.apache.org/iotdb/1.0.1/apache-iotdb-1.0.1-all-bin.zip.sha512">SHA512</a></td>
            <td><a href="https://downloads.apache.org/iotdb/1.0.1/apache-iotdb-1.0.1-all-bin.zip.asc">ASC</a></td>
            <td rowspan="2"><a href="https://www.apache.org/dyn/closer.cgi/iotdb/1.0.1/apache-iotdb-1.0.1-source-release.zip">Sources</a></td>
            <td rowspan="2"><a href="https://downloads.apache.org/iotdb/1.0.1/apache-iotdb-1.0.1-source-release.zip.sha512">SHA512</a></td>
            <td rowspan="2"><a href="https://downloads.apache.org/iotdb/1.0.1/apache-iotdb-1.0.1-source-release.zip.asc">ASC</a></td>
            <td rowspan="2"><a href="https://raw.githubusercontent.com/apache/iotdb/v1.0.1/RELEASE_NOTES.md">release notes</a></td>
      </tr>
      <tr>
            <td><a href="https://www.apache.org/dyn/closer.cgi/iotdb/1.0.0/apache-iotdb-1.0.0-grafana-plugin-bin.zip">Grafana-plugin</a></td>
            <td><a href="https://downloads.apache.org/iotdb/1.0.0/apache-iotdb-1.0.0-grafana-plugin-bin.zip.sha512">SHA512</a></td>
            <td><a href="https://downloads.apache.org/iotdb/1.0.0/apache-iotdb-1.0.0-grafana-plugin-bin.zip.asc">ASC</a></td>
      </tr>
      <tr>
            <td rowspan="3">0.13.4</td>
            <td><a href="https://dlcdn.apache.org/iotdb/0.13.4/apache-iotdb-0.13.4-all-bin.zip">All-in-one</a></td>
            <td><a href="https://dlcdn.apache.org/iotdb/0.13.4/apache-iotdb-0.13.4-all-bin.zip.sha512">SHA512</a></td>
            <td><a href="https://dlcdn.apache.org/iotdb/0.13.4/apache-iotdb-0.13.4-all-bin.zip.asc">ASC</a></td>
            <td rowspan="3"><a href="https://dlcdn.apache.org/iotdb/0.13.4/apache-iotdb-0.13.4-source-release.zip">Sources</a></td>
            <td rowspan="3"><a href="https://dlcdn.apache.org/iotdb/0.13.4/apache-iotdb-0.13.4-source-release.zip.sha512">SHA512</a></td>
            <td rowspan="3"><a href="https://dlcdn.apache.org/iotdb/0.13.4/apache-iotdb-0.13.4-source-release.zip.asc">ASC</a></td>
            <td rowspan="3"><a href="https://archive.apache.org/dist/iotdb/0.13.4/RELEASE_NOTES.md">release notes</a></td>
      </tr>
      <tr>
            <td><a href="https://dlcdn.apache.org/iotdb/0.13.4/apache-iotdb-0.13.4-grafana-connector-bin.zip">Grafana-connector</a></td>
            <td><a href="https://dlcdn.apache.org/iotdb/0.13.4/apache-iotdb-0.13.4-grafana-connector-bin.zip.sha512">SHA512</a></td>
            <td><a href="https://dlcdn.apache.org/iotdb/0.13.4/apache-iotdb-0.13.4-grafana-connector-bin.zip.asc">ASC</a></td>
      </tr>
      <tr>
            <td><a href="https://www.apache.org/dyn/closer.cgi/iotdb/0.13.3/apache-iotdb-0.13.3-grafana-plugin-bin.zip">Grafana-plugin</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.13.3/apache-iotdb-0.13.3-grafana-plugin-bin.zip.sha512">SHA512</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.13.3/apache-iotdb-0.13.3-grafana-plugin-bin.zip.asc">ASC</a></td>
      </tr>
</table>

Legacy version are available here: [https://archive.apache.org/dist/iotdb/](https://archive.apache.org/dist/iotdb/)

## Configurations

- Recommended OS parameters
  * Set the somaxconn as 65535 to avoid "connection reset" error when the system is under high load.
    ```
    # Linux
    > sudo sysctl -w net.core.somaxconn=65535
     
    # FreeBSD or Darwin
    > sudo sysctl -w kern.ipc.somaxconn=65535
    ```

## About Version 1.0

**After we release version 1.0, how to upgrade from v.13.x to v1.0.x?**
  
  - **Version 1.0 has changed the SQL syntax conventions (please refer to the syntax conventions section of the user manual)**.
  - In order to ensure the stability of UDF-related APIs, in version 1.0, UDF-related APIs are seperated into an independent module and no longer depend on the tsfile package. The implemented UDFs need to rewrite the code, replace `TsDataType` with `Type`, and replace `org .apache.iotdb.tsfile.utils.Binary` with `org.apache.iotdb.udf.api.type.Binary`, then redo the packaging and loading process.

### Detailed description of Syntax Conventions in version 1.0 that are different from older versions

In previous versions of syntax conventions, we introduced some ambiguity to maintain compatibility. To avoid ambiguity, we have designed new syntax conventions, and this chapter will explain the issues with the old syntax conventions and why we made the change.

#### Issues related to identifier

In version 0.13 and earlier, identifiers (including path node names) that are not quoted with backquotes are allowed to be pure numbers(Pure numeric path node names need to be enclosed in backquotes in the `SELECT` clause), and are allowed to contain some special characters. **In version 1.0, identifiers that are not quoted with backquotes are not allowed to be pure numbers and only allowed to contain letters, Chinese characters, and underscores.**

#### Issues related to node name

In previous versions of syntax conventions, when do you need to add quotation marks to the node name, and the rules for using single and double quotation marks or backquotes are complicated. We have unified usage of quotation marks in the new syntax conventions. For details, please refer to the relevant chapters of this document.

##### When to use single and double quotes and backquotes

In previous versions of syntax conventions, path node names were defined as identifiers, but when the path separator . was required in the path node name, single or double quotes were required. This goes against the rule that identifiers are quoted using backquotes.

```SQL
# In the previous syntax convention, if you need to create a time series root.sg.`www.baidu.com`, you need to use the following statement:
create root.sg.'www.baidu.com' with datatype=BOOLEAN, encoding=PLAIN

# The time series created by this statement is actually root.sg.'www.baidu.com', that is, the quotation marks are stored together. The three nodes of the time series are {"root","sg","'www.baidu.com'"}.

# In the query statement, if you want to query the data of the time series, the query statement is as follows:
select 'www.baidu.com' from root.sg;
```

**In the 1.0 syntax conventions, special node names are uniformly quoted using backquotes:**

```SQL
# In the new syntax convention, if you need to create a time series root.sg.`www.baidu.com`, the syntax is as follows:
create root.sg.`www.baidu.com` with 'datatype' = 'BOOLEAN', 'encoding' = 'PLAIN'

#To query the time series, you can use the following statement:
select `www.baidu.com` from root.sg;
```

##### The issues of using quotation marks inside node names

In previous versions of syntax conventions, when single quotes ' and double quotes " are used in path node names, they need to be escaped with a backslash \, and the backslashes will be stored as part of the path node name. Other identifiers do not have this restriction, causing inconsistency.

```SQL
# Create time series root.sg.\"a
create timeseries root.sg.`\"a` with datatype=TEXT,encoding=PLAIN;

# Query time series root.sg.\"a
select `\"a` from root.sg;
+-----------------------------+-----------+
|                         Time|root.sg.\"a|
+-----------------------------+-----------+
|1970-01-01T08:00:00.004+08:00|       test|
+-----------------------------+-----------+
```

**In the 1.0 syntax convention, special path node names are uniformly referenced with backquotes.** When single and double quotes are used in path node names, there is no need to add backslashes to escape, and backquotes need to be double-written. For details, please refer to the relevant chapters of the new syntax conventions.

#### Issues related to session API

##### Session API syntax restrictions

In version 0.13, the restrictions on using path nodes in non-SQL interfaces are as follows:

- The node names in path or path prefix as parameter:
  - The node names which should be escaped by backticks (`) in the SQL statement, and escaping is not required here.
  - The node names enclosed in single or double quotes still need to be enclosed in single or double quotes and must be escaped for JAVA strings.
  - For the `checkTimeseriesExists` interface, since the IoTDB-SQL interface is called internally, the time-series pathname must be consistent with the SQL syntax conventions and be escaped for JAVA strings.

**In version 1.0, restrictions on using path nodes in non-SQL interfaces were enhanced:**

- **The node names in path or path prefix as parameter: The node names which should be escaped by backticks (`) in the SQL statement, escaping is required here.**
- **Code example for syntax convention could be found at:** `example/session/src/main/java/org/apache/iotdb/SyntaxConventionRelatedExample.java`

##### Inconsistent handling of string escaping between SQL and Session interfaces

**In previous releases, there was an inconsistency between the SQL and Session interfaces when using strings.** For example, when using SQL to insert Text type data, the string will be unescaped, but not when using the Session interface, which is inconsistent. **In the new syntax convention, we do not unescape the strings. What you store is what will be obtained when querying (for the rules of using single and double quotation marks inside strings, please refer to this document for string literal chapter).**

The following are examples of inconsistencies in the old syntax conventions:

Use Session's insertRecord method to insert data into the time series root.sg.a

```Java
// session insert
String deviceId = "root.sg";
List<String> measurements = new ArrayList<>();
measurements.add("a");
String[] values = new String[]{"\\\\", "\\t", "\\\"", "\\u96d5"};
for(int i = 0; i <= values.length; i++){
  List<String> valueList = new ArrayList<>();
  valueList.add(values[i]);
  session.insertRecord(deviceId, i + 1, measurements, valueList);
  }
```

Query the data of root.sg.a, you can see that there is no unescaping:

```Plain%20Text
// query result
+-----------------------------+---------+
|                         Time|root.sg.a|
+-----------------------------+---------+
|1970-01-01T08:00:00.001+08:00|       \\|
|1970-01-01T08:00:00.002+08:00|       \t|
|1970-01-01T08:00:00.003+08:00|       \"|
|1970-01-01T08:00:00.004+08:00|   \u96d5|
+-----------------------------+---------+
```

Instead use SQL to insert data into root.sg.a:

```SQL
# SQL insert
insert into root.sg(time, a) values(1, "\\")
insert into root.sg(time, a) values(2, "\t")
insert into root.sg(time, a) values(3, "\"")
insert into root.sg(time, a) values(4, "\u96d5")
```

Query the data of root.sg.a, you can see that the string is unescaped:

```Plain%20Text
// query result
+-----------------------------+---------+
|                         Time|root.sg.a|
+-----------------------------+---------+
|1970-01-01T08:00:00.001+08:00|        \|
|1970-01-01T08:00:00.002+08:00|         |
|1970-01-01T08:00:00.003+08:00|        "|
|1970-01-01T08:00:00.004+08:00|       雕|
+-----------------------------+---------+
```


## How to Upgrade

- How to upgrade a minor version (e.g., from v0.12.3 to v0.12.5)?
  * versions which have the same major version are compatible.
  * Just download and unzip the new version. Then modify the configuration files to keep consistent
    with what you set in the old version.
  * stop the old version instance, and start the new one.

- How to upgrade from v.12.x to v0.13.x?

  * The data format (i.e., TsFile data) of v0.12.x and v0.13.x are compatible, but the WAL file is
    incompatible. So, you can follow the steps:
  * **<font color=red> Execute `SET STSTEM TO READONLY` command in CLI. </font>**
  * **<font color=red> Stop writing new data.</font>**
  * Execute `flush` command to close all TsFiles.
  * We recommend to back up all data files before upgrading for rolling back.
  * Just download, unzip v0.13.x.zip, and modify conf/iotdb-engine.properties, **<font color=red> especially the unchangeable configurations like timestamp precision</font>**. Let all the
    directories point to the data folder set in v0.12.x (or the backup folder). You can also modify
    other settings if you want.
  * Stop IoTDB v0.12.x instance, and then start v0.13.x.
  * **<font color=red>After the steps above, please make sure the `iotdb_version` in `data/system/schema/system.properties` file is `0.13.x`. 
    If not, please change it to `0.13.x` manually.</font>**
  * __NOTICE: V0.13 changes many settings in conf/iotdb-engine.properties, so do not use v0.12's
    configuration file directly.__
  * **In 0.13, the SQL syntax has been changed. The identifiers not enclosed in backquotes can only contain the following characters, otherwise they need to be enclosed in backquotes.**
    * **[0-9 a-z A-Z _ : @ # $ { }] (letters, digits, some special characters)**
    * **['\u2E80'..'\u9FFF'] (UNICODE Chinese characters)**
  * **In 0.13, if the path node name in the `SELECT` clause consists of pure numbers, it needs to be enclosed in backquotes to distinguish it from the constant in the expression. For example, in the statement "select 123 + \`123\` from root.sg", the former 123 represents a constant, and the latter \`123\` will be spliced with root.sg, indicating the path root.sg.\`123\`.**

- How to upgrade from v.11.x or v0.10.x to v0.12.x?
  * Upgrading from v0.11 or v0.10 to v0.12 is similar as v0.9 to v0.10. The upgrade tool will rewrite the data files automatically.
  * **<font color=red>Stop writing new data.</font>**
  * Call `flush` command using sbin/start-cli.sh in original version to close all TsFiles.
  * We recommend to backup the data file (also the wal files and mlog.txt) before upgrading for rolling back.
  * Just download, unzip v0.12.x.zip, and modify conf/iotdb-engine.proeprties to let all the 
  directories point to the folders set in previous version (or the backup folder). 
  You can also modify other settings if you want. Any other config changes in v0.11 should be moved to v0.12. 
  * Stop IoTDB v0.11 or v0.10 instance, and start v0.12.x, then the IoTDB will upgrade data file format automatically. It is ok to read and write data when the upgrading process works.
  * After a log `All files upgraded successfully!` printed, the upgrading completes.
  * __NOTICE 1: V0.12 changes many settings in conf/iotdb-engine.properties, so do not use previous 
    configuration file directly.__
  * __NOTICE 2: V0.12 doesn't support upgrade from v0.9 or lower version, please upgrade to v0.10 first if needed.__
  * __NOTICE 3: We don't recommend deleting data before the upgrading finished. The deletion will fail if you try to delete data in the database with upgrading files.__

- How to upgrade from v.10.x to v0.11.x?
  * The data format (i.e., TsFile data) of v0.10.x and v0.11 are compatible, but the WAL file is 
  incompatible. So, you can follow the steps:
  * **<font color=red>Stop writing new data.</font>**
  * Call `flush` command using `sbin/start-cli.sh` in v0.10.x to close all TsFiles.
  * We recommend to backup the wal files and mlog.txt before upgrading for rolling back.
  * Just download, unzip v0.11.x.zip, and modify conf/iotdb-engine.properties to let all the 
    directories point to the data folder set in v0.10.x (or the backup folder). You can also modify 
    other settings if you want.
  * Stop IoTDB v0.10.x instance, and start v0.11.x, then the IoTDB will upgrade data file format 
    automatically.
  * __NOTICE: V0.11 changes many settings in conf/iotdb-engine.properties, so do not use v0.10's 
    configuration file directly.__

- How to upgrade from v0.9.x to v0.10.x?
  * Upgrading from v0.9 to v0.10 is more complex than v0.8 to v0.9.
  * **<font color=red>Stop writing new data.</font>**
  * Call `flush` command using sbin/start-client.sh in v0.9 to close all TsFiles.
  * We recommend to backup the data file (also the wal files and mlog.txt) before upgrading for rolling back.
  * Just download, unzip v0.10.x.zip, and modify conf/iotdb-engine.proeprties to let all the 
  directories point to the folders set in v0.9.x  (or the backup folder). 
  You can also modify other settings if you want. 
  * Stop IoTDB v0.9 instance, and start v0.10.x, then the IoTDB will upgrade data file format automatically.

- How to upgrade from 0.8.x to v0.9.x?
  * We recommend to backup the data file (also the wal files and mlog.txt) before upgrading for rolling back.
  * Just download, unzip v0.9.x.zip, and modify conf/iotdb-engine.properties to let all the 
  directories point to the folders set in v0.8.x (or the backup folder). 
  You can also modify other settings if you want. 
  * Stop IoTDB v0.8 instance, and start v0.9.x, then the IoTDB will upgrade data file format automatically.


​       

# All releases

Find all releases in the [Archive repository](https://archive.apache.org/dist/iotdb/).



# Verifying Hashes and Signatures

Along with our releases, we also provide sha512 hashes in *.sha512 files and cryptographic signatures in *.asc files. The Apache Software Foundation has an extensive tutorial to [verify hashes and signatures ](http://www.apache.org/info/verification.html)which you can follow by using any of these release-signing [KEYS ](https://downloads.apache.org/iotdb/KEYS).
