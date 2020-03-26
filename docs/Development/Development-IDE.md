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

# 一、How to contribute

## Resources

Home Page：https://iotdb.apache.org/

Repository：https://github.com/apache/incubator-iotdb/tree/master

Quick start：http://iotdb.apache.org/document/master/UserGuide/0-Get%20Started/1-QuickStart.html

Jira：https://issues.apache.org/jira/projects/IOTDB/issues/IOTDB-9?filter=allopenissues

Wiki：https://cwiki.apache.org/confluence/display/IOTDB/Home

## Subscribe mail list

Discuss: dev@iotdb.apache.org

send an email to dev-subscribe@iotdb.apache.org, then reply again.

Jira report: notifications@iotdb.apache.org

send an email to notifications-subscribe@iotdb.apache.org, then reply again.

## Issue report

Jira or Github issue.

The jira issue change will auto sent an email to notifications@iotdb.apache.org

## Contribute Documents

All documents in website is in docs in repository

Doc version -> branch

* In progress -> master
* major_version.x -> rel/major_version （e.g., 0.9.x -> rel/0.9）

Note:

* The pictures in markdown could be put into https://github.com/thulab/iotdb/issues/543 to get url

## Contribute codes

* Leave a comment in the jira issue you want to work.
* Import code style in idea: java-google-style.xml
* Submit PR, use [IOTDB-issue number] as prefix

## Import IoTDB in IDE

### IDEA

* "File" -> "Open" -> choose the root path of IoTDB source code. 
* use `mvn clean compile -Dmaven.test.skip=true`to get target.
* mark directory ***server/target/generated-sources/antlr4*** as source code
* mark directory ***service-rpc/target/generated-sources/thrift*** as source code 

### Eclipse

Using Eclipse to develop IoTDB is also simple but requires some plugins of Eclipse.

- If your Eclipse version is released before 2019, Antlr plugin maybe not work in Eclipse. In this way, you have to run the command in your console first: `mvn eclipse:eclipse -DskipTests`. 
After the command is done, you can import IoTDB as an existing project:
  - Choose menu "import" -> "General" -> "Existing Projects into Workspace" -> Choose IoTDB
   root path;
  - Done.

- If your Eclipse version is fashion enough (e.g., you are using the latest version of Eclipse),
you can just choose menu "import" -> "Maven" -> "Existing Maven Projects".
 

## Debugging IoTDB

The main class of IoTDB server is `org.apache.iotdb.db.service.IoTDB`.
The main class of IoTDB cli is `org.apache.iotdb.client.Client` 
(or `org.apache.iotdb.client.WinClient` on Win OS).

The client needs parameter: "-h 127.0.0.1 -p 6667 -u root -pw root"

You can run/debug IoTDB by using the two classes as the entrance.

Another way to understand IoTDB is to read and try Unit Tests.

* The implementation of RPC in the server ```server/src/main/java/org/apache/iotdb/db/service/TSServiceImpl```
	* JDBC.execute() -> TSServiceImpl.executeStatement(TSExecuteStatementReq req)
	* JDBC.executeQuery() -> TSServiceImpl.executeQueryStatement(TSExecuteStatementReq req)	
	* Session.insert() -> TSServiceImpl.insert(TSInsertReq req)

* Storage Engine: org.apache.iotdb.db.engine.StorageEngine
* Query Engine: org.apache.iotdb.db.qp.Planner

