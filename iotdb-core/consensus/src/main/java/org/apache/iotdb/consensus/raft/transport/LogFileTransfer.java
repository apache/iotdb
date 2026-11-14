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

package org.apache.iotdb.consensus.raft.transport;

import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq;
import org.apache.iotdb.consensus.raft.thrift.TAppendEntriesReq;
import org.apache.iotdb.consensus.raft.thrift.TRaftLogEntry;

import java.io.IOException;

/** Optional transport encoding; ordinary Raft sends its original entries. */
public interface LogFileTransfer {
  byte[] prepare(AppendEntriesReq request) throws IOException;

  AppendEntriesReq decode(TAppendEntriesReq request) throws IOException;

  long receivedFiles();

  static long payloadBytes(TAppendEntriesReq request) {
    if (request.isSetLogFile()) {
      return request.bufferForLogFile().remaining();
    }
    long bytes = 0;
    for (TRaftLogEntry entry : request.getEntries()) {
      bytes += 25L + entry.bufferForPayload().remaining();
      if (entry.isSetProfileMeta()) {
        bytes += entry.bufferForProfileMeta().remaining();
      }
    }
    return bytes;
  }
}
