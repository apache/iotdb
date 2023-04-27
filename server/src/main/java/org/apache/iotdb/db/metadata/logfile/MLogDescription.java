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

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * The data structure of the MLog description file.
 *
 * <ul>
 *   <li>1 long (8 bytes): check point of MLog started from 0, which means that MLog entries with
 *       offset less than the checkpoint do not need to be redone
 * </ul>
 */
public class MLogDescription {

  private long checkPoint;

  public int getSize() {
    return 8;
  }

  public long getCheckPoint() {
    return checkPoint;
  }

  public void setCheckPoint(long checkPoint) {
    this.checkPoint = checkPoint;
  }

  public void serialize(FileChannel fileChannel) throws IOException {
    fileChannel.position(0);
    ByteBuffer buffer = ByteBuffer.allocate(8);
    ReadWriteIOUtils.write(getCheckPoint(), buffer);
    buffer.flip();
    fileChannel.write(buffer);
  }

  public void deserialize(InputStream inputStream) throws IOException {
    this.checkPoint = ReadWriteIOUtils.readLong(inputStream);
  }
}
