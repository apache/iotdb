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

# WAL

## Working Process
* WAL overall recording principle
  * For each Memtable, a corresponding WAL file will be recorded. When the Memtable is flushed, the WAL will be deleted.
* WAL record details
  * The test workload is 1sg,1device,100sensor,1,000,000 points each sensor,force_wal_period_in_ms=10
  * In org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode, the WAL buffer size will be allocated according to the wal_buffer_size in the configuration. If the buffer size is exceeded during the process of putting WAL, it will be flushed to disk
  * In org.apache.iotdb.db.writelog.manager, nodeMap will continue to accumulate WAL
  * WAL has three ways to be flashed to disk (enable at the same time)
    * In org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode, half of the wal_buffer_size in the configuration will be allocated as the WAL logBufferWorking writable buffer, and the other half will be used as the flashing buffer area, such as adding WAL In the process, if the logBufferWorking size is exceeded, it will be flushed to the disk.
    * Each time a record is written in org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode, it will be judged whether the accumulated WAL size of the current node exceeds the flush_wal_threshold in the configuration. If it exceeds, it will be flushed to the disk.
    * When org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager starts, a timing thread will be generated, call the thread regularly every force_wal_period_in_ms interval to flush the nodeMap in the memory to the disk .

## Test Result

* The main time spent on forceTask is focused on. org.apache.iotdb.db.writelog.io.LogWriter.force()
* Test forceTask on SSD and HDD respectively
  * In SSD, the speed is 75MB/s
  * In HDD, the speed is 5MB/s
  * So when in HDD, users must pay attention to adjustment force_wal_period_in_ms and let it not to be too small, otherwise it will seriously reduce write performance
    * After testing, the optimal parameter configuration in HDD is 100ms-200ms, and the test results are as follows
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/24886743/93157479-e3319f80-f73c-11ea-836f-459d03cb2fab.png">

## Related Code

* org.apache.iotdb.db.writelog.*
