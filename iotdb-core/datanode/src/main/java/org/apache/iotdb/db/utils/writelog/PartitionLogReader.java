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

package org.apache.iotdb.db.utils.writelog;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.FileTimeIndex;

public class PartitionLogReader implements AutoCloseable {

  private DataInputStream logStream;
  private String filepath;

  private byte[] buffer;

  public PartitionLogReader(File logFile) throws IOException {
    logStream =
        new DataInputStream(new BufferedInputStream(Files.newInputStream(logFile.toPath())));
    this.filepath = logFile.getPath();
  }

  public Map<TsFileID, FileTimeIndex> read() throws IOException {
    // read the log file
    // return the TsFileProcessor map
    return null;
  }

  @Override
  public void close() throws Exception {
    logStream.close();
  }
}
