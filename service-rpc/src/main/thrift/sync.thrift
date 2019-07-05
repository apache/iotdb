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

enum SyncDataStatus {
  SUCCESS_STATUS,
  FINISH_STATUS,
  PROCESSING_STATUS
}

service SyncService{
	bool checkIdentity(1:string uuid, 2:string address)
	string syncSchema(1:string md5, 2:binary buff, 3:SyncDataStatus status)
	string syncData(1:string md5, 2:list<string> filename, 3:binary buff, 4:SyncDataStatus status)
	bool load()
	void cleanUp()
	bool init(1:string storageGroupName)
}