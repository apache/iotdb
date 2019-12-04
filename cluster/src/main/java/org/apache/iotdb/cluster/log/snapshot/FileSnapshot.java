/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.snapshot;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

/**
 * FileSnapshot records the data files in a socket and their md5 (or other verification).
 * When the snapshot is used to perform a catch-up, the receiver should:
 * 1. create a remote snapshot indicating that the socket is being pulled from the remote
 * 2. traverse the file list, for each file:
 *  2.1 if the file exists locally and the md5 is correct, skip it.
 *  2.2 otherwise pull the file from the remote.
 * 3. replace the remote snapshot with a FileSnapshot indicating that the socket of this node is
 * synchronized with the remote one.
 * TODO-Cluster: implement and use this
 */
public class FileSnapshot extends Snapshot {

  private List<TsFileResource> dataFiles;
  private List<byte[]> md5s;

  public FileSnapshot() {
    dataFiles = new ArrayList<>();
    md5s = new ArrayList<>();
  }

  public void addFile(TsFileResource resource) {
    dataFiles.add(resource);
    md5s.add(getFileMd5(resource));
  }

  private byte[] getFileMd5(TsFileResource resource) {
    // TODO-Cluster: implement
    return new byte[0];
  }

  @Override
  public ByteBuffer serialize() {
    // TODO-Cluster: implement
    return null;
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    // TODO-Cluster: implement
  }
}
