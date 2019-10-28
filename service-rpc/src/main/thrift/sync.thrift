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

typedef i32 int 
typedef i16 short
typedef i64 long

struct ResultStatus{
  bool success,
  string msg,
  i32 errorCode
}

service SyncService{
	ResultStatus check(1:string address, 2:string uuid)
	ResultStatus startSync();
	ResultStatus init(1:string storageGroupName)
	ResultStatus syncDeletedFileName(1:string fileName)
	ResultStatus initSyncData(1:string filename)
	ResultStatus syncData(1:binary buff)
	ResultStatus checkDataMD5(1:string md5)
	ResultStatus endSync()
}