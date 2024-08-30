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

package org.apache.iotdb.db.pipe.consensus.deletion.recover;

import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class DeletionReader implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeletionReader.class);
  private final Consumer<DeletionResource> removeHook;
  private final FileInputStream fileInputStream;
  private final FileChannel fileChannel;

  public DeletionReader(File logFile, Consumer<DeletionResource> removeHook) throws IOException {
    this.fileInputStream = new FileInputStream(logFile);
    this.fileChannel = fileInputStream.getChannel();
    this.removeHook = removeHook;
  }

  public List<DeletionResource> readAllDeletions() throws IOException {
    // Read metaData
    ByteBuffer intBuffer = ByteBuffer.allocate(4);
    fileChannel.read(intBuffer);
    intBuffer.flip();
    int deletionNum = intBuffer.getInt();

    // Read deletions
    long remainingBytes = fileChannel.size() - fileChannel.position();
    ByteBuffer byteBuffer = ByteBuffer.allocate((int) remainingBytes);
    fileChannel.read(byteBuffer);
    byteBuffer.flip();

    List<DeletionResource> deletions = new ArrayList<>();
    for (int i = 0; i < deletionNum; i++) {
      deletions.add(DeletionResource.deserialize(byteBuffer, removeHook));
    }
    return deletions;
  }

  @Override
  public void close() throws IOException {
    this.fileInputStream.close();
    this.fileChannel.close();
  }
}
