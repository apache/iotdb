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
package org.apache.iotdb.consensus.common;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

public class SnapshotMeta {
  private ByteBuffer metadata;
  private List<File> snapshotFiles;

  public SnapshotMeta(ByteBuffer metadata, List<File> snapshotFiles) {
    this.metadata = metadata;
    this.snapshotFiles = snapshotFiles;
  }

  public ByteBuffer getMetadata() {
    return metadata;
  }

  public void setMetadata(ByteBuffer metadata) {
    this.metadata = metadata;
  }

  public List<File> getSnapshotFiles() {
    return snapshotFiles;
  }

  public void setSnapshotFiles(List<File> snapshotFiles) {
    this.snapshotFiles = snapshotFiles;
  }
}
