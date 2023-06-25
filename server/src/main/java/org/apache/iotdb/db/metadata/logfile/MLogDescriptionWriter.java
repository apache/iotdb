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

package org.apache.iotdb.db.metadata.logfile;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class MLogDescriptionWriter {

  private final FileChannel fileChannel;

  private final MLogDescription mlogdescription;

  public MLogDescriptionWriter(String schemaDir, String logFileName) throws IOException {
    File file = new File(schemaDir, logFileName);
    if (!file.exists() && !file.createNewFile()) {
      throw new IOException(
          String.format(
              "Failed to create file %s because the named file already exists", file.getName()));
    }
    fileChannel =
        FileChannel.open(new File(schemaDir, logFileName).toPath(), StandardOpenOption.WRITE);
    mlogdescription = new MLogDescription();
  }

  public synchronized void updateCheckPoint(long checkPoint) throws IOException {
    mlogdescription.setCheckPoint(checkPoint);
    mlogdescription.serialize(fileChannel);
  }

  public synchronized void close() throws IOException {
    fileChannel.close();
  }
}
