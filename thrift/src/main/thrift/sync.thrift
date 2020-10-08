/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
namespace java org.apache.iotdb.service.sync.thrift

struct SyncStatus{
  1:required i32 code
  2:required string msg
}

// The sender and receiver need to check some info to confirm validity
struct ConfirmInfo{
  // check whether the ip of sender is in thw white list of receiver.
  1:string address

  // Sender needs to tell receiver its identity.
  2:string uuid

  // The partition interval of sender and receiver need to be the same.
  3:i64 partitionInterval

  // The version of sender and receiver need to be the same.
  4:string version
}

service SyncService{
	SyncStatus check(ConfirmInfo info)
	SyncStatus startSync();
	SyncStatus init(1:string storageGroupName)
	SyncStatus syncDeletedFileName(1:string fileName)
	SyncStatus initSyncData(1:string filename)
	SyncStatus syncData(1:binary buff)
	SyncStatus checkDataMD5(1:string md5)
	SyncStatus endSync()
}