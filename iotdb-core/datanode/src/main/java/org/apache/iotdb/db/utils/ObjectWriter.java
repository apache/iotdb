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
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;

public class ObjectWriter implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ObjectWriter.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final FileOutputStream fos;

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
    fos = new FileOutputStream(filePath, true);
  }

  public void write(boolean isGeneratedByConsensus, long offset, byte[] content)
      throws IOException {
    if (file.length() != offset) {
      if (isGeneratedByConsensus || offset == 0) {
        fos.getChannel().truncate(offset);
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
    fos.write(content);
  }

  @Override
  public void close() throws Exception {
    fos.close();
  }
}
