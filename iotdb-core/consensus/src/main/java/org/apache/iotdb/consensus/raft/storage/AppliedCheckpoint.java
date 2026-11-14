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

package org.apache.iotdb.consensus.raft.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * {@code <groupDir>/applied.ckpt}: diagnostic applied progress. This is not a state-machine
 * snapshot and is never used to skip replay when rebuilding a DataRegion after restart.
 */
public final class AppliedCheckpoint {

  public static final String FILE_NAME = "applied.ckpt";

  private final File file;

  public AppliedCheckpoint(File groupDir) {
    this.file = new File(groupDir, FILE_NAME);
  }

  /** -1 when absent. */
  public long load() throws IOException {
    byte[] p = AtomicFiles.read(file);
    return p == null || p.length != 8 ? -1 : ByteBuffer.wrap(p).getLong();
  }

  public void save(long lastApplied) throws IOException {
    AtomicFiles.write(file, ByteBuffer.allocate(8).putLong(lastApplied).array());
  }
}
