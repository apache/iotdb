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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;

import org.apache.tsfile.external.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public class ObjectWriter implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ObjectWriter.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final FileChannel channel;

  private final File file;

  public ObjectWriter(File filePath) throws FileNotFoundException {
    try {
      FileUtils.forceMkdir(filePath.getParentFile());
    } catch (final IOException e) {
      throw new FileNotFoundException(
          DataNodeMiscMessages.ERROR_OCCURRED_DURING_CREATING_DIR + filePath);
    }
    if (!Files.exists(filePath.toPath())) {
      try {
        Files.createFile(filePath.toPath());
      } catch (IOException e) {
        throw new FileNotFoundException(e.getMessage());
      }
    }
    file = filePath;
    channel = openChannel(filePath);
  }

  public void write(boolean isGeneratedByConsensus, long offset, byte[] content)
      throws IOException {
    if (file.length() != offset) {
      if (isGeneratedByConsensus || offset == 0) {
        org.apache.iotdb.commons.utils.FileUtils.truncateFile(file, offset);
        channel.position(offset);
      } else {
        throw new IOException(
            String.format(
                DataNodeMiscMessages
                    .MISC_EXCEPTION_THE_FILE_LENGTH_S_IS_NOT_EQUAL_TO_THE_OFFSET_S_73905F07,
                file.length(),
                offset));
      }
    }
    if (file.length() + content.length > config.getMaxObjectSizeInByte()) {
      throw new IOException(DataNodeMiscMessages.FILE_LENGTH_LARGER_THAN_MAX);
    }
    ByteBuffer buffer = ByteBuffer.wrap(content);
    while (buffer.hasRemaining()) {
      channel.write(buffer);
    }
  }

  @Override
  public void close() throws Exception {
    channel.close();
  }

  private static FileChannel openChannel(File file) throws FileNotFoundException {
    try {
      FileChannel channel =
          FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
      channel.position(channel.size());
      return channel;
    } catch (IOException e) {
      FileNotFoundException exception = new FileNotFoundException(e.getMessage());
      exception.initCause(e);
      throw exception;
    }
  }
}
