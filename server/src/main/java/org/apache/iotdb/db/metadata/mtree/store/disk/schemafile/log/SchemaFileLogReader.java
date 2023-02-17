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

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.exception.metadata.schemafile.SchemaFileLogCorruptedException;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
    inputStream = logFile.exists() ? new FileInputStream(logFile) : null;
  }

  public List<byte[]> collectUpdatedEntries() throws IOException, SchemaFileLogCorruptedException {
    if (inputStream == null || inputStream.getChannel().size() == 0) {
      return Collections.emptyList();
    }

    // TODO: better performance if not scan from head to tail

    List<byte[]> colBuffers = new ArrayList<>();
    byte[] tempBytes = new byte[SchemaFileConfig.PAGE_LENGTH];
    while (inputStream.available() > 0) {
      inputStream.read(tempBytes, 0, 1);

      if (tempBytes[0] == SchemaFileConfig.SF_COMMIT_MARK) {
        throw new SchemaFileLogCorruptedException(
            logFile.getAbsolutePath(), "COMMIT_MARK without PREPARE_MARK");
      }

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
          throw new SchemaFileLogCorruptedException(
              logFile.getAbsolutePath(),
              "an extraneous byte rather than " + "COMMIT_MARK after PREPARE_MARK");
        }

        // no bytes after commit mark, safe to exit
        if (inputStream.read(tempBytes, 0, 1) < 0) {
          return Collections.emptyList();
        }
      }

      // corrupted within one entry
      if (inputStream.read(tempBytes, 1, tempBytes.length - 1) < tempBytes.length - 2) {
        throw new SchemaFileLogCorruptedException(logFile.getAbsolutePath(), "incomplete entry.");
      }

      colBuffers.add(tempBytes);
      tempBytes = new byte[SchemaFileConfig.PAGE_LENGTH];
    }

    throw new SchemaFileLogCorruptedException(
        logFile.getAbsolutePath(), "not ended by COMMIT_MARK nor PREPARE_MARK.");
  }

  public void close() throws IOException {
    if (inputStream != null) {
      inputStream.close();
    }
  }
}
