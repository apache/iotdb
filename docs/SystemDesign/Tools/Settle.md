<!--

```
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
```

-->

## TsFile Settle Tool

### 1. Introduction

1. The settle tool helps you sort sealed tsFiles and mods files, filter out deleted data and generate new tsFiles.  If all the data in a tsFile is deleted, then the local tsFile and its corresponding resource file will be deleted.  
2. The tsFile settle tool can be used only for IOTDB v0.12, that is, it can only be used for tsFile which version is 3. If the version is later than this version, you must upgrade tsFile to V3 using the online upgrade tool first . 
3. tsFile settle tool can be divided into online settle tool and offline settle tool. Both of them record a settle log ("data\system\settle\settle.txt ") during the settling process, which can be used to recover the failed files next time.  If there are any failed files in the log, they will be settled first.  

### 2. Offline Settle Tool

The offline settle tool is started in the command line, which startup scripts "settle.bat" and "settle.sh" are generated in the "server\target\iotdb-server-{version}\tools\tsFileToolSet" directory after server is compiled. When using the offline settle tool, ensure that the IOTDB server stops running; otherwise, an error may occur. See the UserGuide for more details.

#### 2.1 Related Classes

Settle Utility Class : org.apache.iotdb.db.tools.settle.tsFileAndModSettleTool

### 3. Online Settle Tool

The online settle tool will register a settle service in the background when a user enters the settle command in the IOTDB client. The service finds all tsFiles in the specified storage group and starts a settle thread for each tsFile.  Here's how the settle thread works.

#### 3.1 Workflow of a Settle Thread

1. First add readLock to the old tsFile file to prevent the tsFile from being deleted during the settle process, and the user is allowed to query the tsFile before deleting its local mods file.  [This step is implemented in the settle() method of StorageGroupProcessor class.]

2. Mark the status of this tsFile as 1 in the settle log.

3. Settle the old tsFile with its mods file and rewrite it to the new tsFile (the new tsFile and its corresponding new resource file have been generated and stored in a temporary directory: "old tsFile directory \ new tsFile time partition directory \").  If the old tsFile data has been deleted, the local tsFile and its resource file will be deleted directly. Remember to add writeLock before deleting tsFile and release writeLock after deleting.  [This step is implemented in settleOnetsFileAndMod() method of the tsFileAndModSettleTool class.]

4. Mark the status of this tsFile as 2 in the settle log.

5. Add writeLock to the old tsFile, block user query and other operations.[This step is implemented in settletsFileCallBack() method of the StorageGroupProcessor class.]

6. Delete the old mods file and then move the new tsFile and its corresponding new resource file to the correct directory by deleting the corresponding old files and then moving the new one. [This step is implemented in moveNewtsFile() method of tsFileRewriteTool class.] 

7. Clear the cache data related to the tsFile in memory, that is, the data in memory related to the rewritten tsFile should be updated in time  ：

   (1) Clear cache (including chunk cache and timeseriesmetadata cache)，to prevent reading deleted data from the tsFile because the mods file was deleted.

   (2) If the tsFile is deleted after settling，then remove the oldtsFileResource from tsFileManagement in StorageGroupProcessor.[This step is implemented in settletsFileCallBack() method of the StorageGroupProcessor class.]

8. Release writeLock on the tsFile to allow the user to query the file, and then release readLock to allow the file to be deleted. 

9. Mark the status of this tsFile as 3 in the settle log，settle completed！

#### 3.2 Note

​	The online settle tool blocks users from deleting the data in the virtual storage group until the settle is completed.  

#### 3.3 Supplementary

1. The situation when the local tsFile corresponding to OldtsFileResource does not exist : 

   (1) In the process of settling last time, the old tsFile was deleted because the data was deleted and the relevant files were deleted. Before the settle process was completed, an exception occurred and the settle thread was interrupted.  The local file corresponding to the tsFile settle record in the settle.txt log does not exist after the next startup.  

   (2) In the last process of settling, the old tsFile was deleted and the thread broke unexpectedly before the new tsFile was moved.  

2. The situation of NewtsFileResource data after settling :

   (1) If the old tsFile is not sealed, then it will not be settling and null is returned. 

   (2) If the old tsFile does not have a mods file, that is, no record is deleted, then it will not be settling and null is returned. 

   (3) After settling, all data in the old tsFile is deleted, the local tsFile and resource file are deleted, and no new tsFile is generated, and null is returned.  

#### 3.4 Related Classes

1. Settle Service Class：org.apache.iotdb.db.service.SettleService
2. Settle Thread Class：org.apache.iotdb.db.engine.settle.SettleTask
3. Settle Log Class：org.apache.iotdb.db.engine.settle.SettleLog
4. Settle Utility Class：org.apache.iotdb.db.tools.settle.tsFileAndModSettleTool

#### 3.5 Settle Log

1. The path to the settle log file is "data\system\settle\settle.txt", which contains the settle status of a tsFile in each entry and the format is ：<tsFile path>,<status>

2. There are three statuses of settle. Each status represents the following meanings:

   (1) Status 1：indicates that the tsFile is being settled and no new tsFile has been generated.

   (2) Status 2：indicates that the new tsFile has been generated, that is, the settle thread has settled the data into the new tsFile and has not moved it to the correct directory.  

   (3) Status 3：indicates that the mods file corresponding to the old tsFile has been deleted, and the new tsFile and its corresponding resource file have been moved to the correct directory.  

3. When all tsFiles in this settle service have been settled successfully, the settle log settle.txt file will be deleted.  

4. The settle log can be used to recover tsFiles that failed to be settled during  next restart. The detail is as following : 

   1) When settle service starts, it checks for the existence of the settle.txt file, and if so, it reads tsFiles that failed to be settled in the log and adds them to the settle list.  

   2) Start settling：

   ​	(1) For the tsFile whose status is 1, it will be deleted and resettled to the new tsFile, its corresponding old mods files will be deleted, and the new tsFile and resource files will be moved to the correct directory.  

   ​	(2) For the tsFile whose state is 2,  which indicating that its new tsFile and corresponding resource file have been generated, so the settle thread can simply move them to the correct directory.  

