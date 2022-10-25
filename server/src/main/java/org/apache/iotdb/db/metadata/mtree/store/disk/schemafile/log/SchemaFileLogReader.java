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

package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.log;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.metadata.logfile.IDeserializer;
import org.apache.iotdb.db.metadata.logfile.SchemaLogReader;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaFile;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaPage;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Load bytes from SchemaFileLog while assuring integrity of entries and marks, leaving semantic
 * correctness of entry bytes unchecked.
 */
public class SchemaFileLogReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaFileLogReader.class);

  private final File logFile;

  private final FileInputStream inputStream;

  public SchemaFileLogReader(String logFilePath) throws IOException {
    logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);
    inputStream = new FileInputStream(logFile);
  }

  public Collection<byte[]> collectUpdatedEntries() throws IOException {
    List<byte[]> colBuffers = new ArrayList<>();
    byte[] tempBytes = new byte[SchemaFileConfig.PAGE_LENGTH];
    while (inputStream.available() > 0) {
      int i2 = inputStream.read(tempBytes, 0, 1);

      // handle prepare mark
      if (tempBytes[0] == SchemaFileConfig.SF_PREPARE_MARK) {

        // nothing after then, restore as what logged
        if (inputStream.available() < 1) {
          return colBuffers;
        }

        // remove what is collected if commit mark followed, throw exception otherwise
        inputStream.read(tempBytes, 0, 1);
        if (tempBytes[0] == SchemaFileConfig.SF_COMMIT_MARK) {
          colBuffers.clear();
        } else {
          LOGGER.error("SchemaFileLog {} corrupted since an extraneous byte rather " +
              "than COMMIT_MARK after PREPARE_MARK", logFile.getPath());
          throw new IOException("SchemaFileLog corrupted.");
        }

        // no bytes after commit mark, safe to exit
        if (inputStream.read(tempBytes, 0, 1) < 0) {
          return Collections.emptySet();
        }
      }

      // corrupted within one entry
      int cnt = inputStream.read(tempBytes, 1, tempBytes.length - 1);
      if (cnt < tempBytes.length - 2) {
        LOGGER.error("SchemaFileLog {} corrupted for an incomplete entry.");
        throw new EOFException();
      }

      colBuffers.add(tempBytes);
      tempBytes = new byte[SchemaFileConfig.PAGE_LENGTH];
    }

    LOGGER.error("SchemaFileLog {} corrupted since not ended by COMMIT_MARK nor PREPARE_MARK.", logFile.getPath());
    throw new EOFException();
  }

  public void close() throws IOException {
    inputStream.close();
  }
}
