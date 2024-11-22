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
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResourceManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class DeletionReader implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeletionReader.class);
  private static final int MAGIC_STRING_BYTES_SIZE =
      DeletionResourceManager.MAGIC_VERSION_V1.getBytes(StandardCharsets.UTF_8).length;
  private final String regionId;
  private final Consumer<DeletionResource> removeHook;
  private final File logFile;
  private final FileInputStream fileInputStream;
  private final FileChannel fileChannel;

  public DeletionReader(File logFile, String regionId, Consumer<DeletionResource> removeHook)
      throws IOException {
    this.logFile = logFile;
    this.regionId = regionId;
    this.fileInputStream = new FileInputStream(logFile);
    this.fileChannel = fileInputStream.getChannel();
    this.removeHook = removeHook;
  }

  public List<DeletionResource> readAllDeletions() throws IOException {
    try {
      // Read magic string
      ByteBuffer magicStringBuffer = ByteBuffer.allocate(MAGIC_STRING_BYTES_SIZE);
      fileChannel.read(magicStringBuffer);
      magicStringBuffer.flip();
      String magicVersion = new String(magicStringBuffer.array(), StandardCharsets.UTF_8);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Read deletion file-{} magic version: {}", logFile, magicVersion);
      }

      // Read deletions
      long remainingBytes = fileChannel.size() - fileChannel.position();
      ByteBuffer byteBuffer = ByteBuffer.allocate((int) remainingBytes);
      fileChannel.read(byteBuffer);
      byteBuffer.flip();

      List<DeletionResource> deletions = new ArrayList<>();

      while (byteBuffer.hasRemaining()) {
        DeletionResource deletionResource =
            DeletionResource.deserialize(byteBuffer, regionId, removeHook);
        deletions.add(deletionResource);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Read deletion: {} from file {}", deletionResource, logFile);
        }
      }
      return deletions;
    } catch (IOException e) {
      // if file is corrupted, throw an exception and skip subsequence DAL.
      LOGGER.warn(
          "Failed to read deletion file {}, may because this file corrupted when writing it.",
          logFile,
          e);
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    this.fileInputStream.close();
    this.fileChannel.close();
  }
}
