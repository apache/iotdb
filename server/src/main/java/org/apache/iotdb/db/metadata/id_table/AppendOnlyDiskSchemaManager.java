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

package org.apache.iotdb.db.metadata.id_table;

import org.apache.iotdb.db.metadata.id_table.entry.DiskSchemaEntry;
import org.apache.iotdb.db.utils.TestOnly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** store id table schema in append only file */
public class AppendOnlyDiskSchemaManager implements DiskSchemaManager {

  private static final String FILE_NAME = "SeriesKeyMapping.meta";

  File dataFile;

  OutputStream outputStream;

  long loc;

  private static final Logger logger = LoggerFactory.getLogger(AppendOnlyDiskSchemaManager.class);

  public AppendOnlyDiskSchemaManager(File dir) {
    try {
      initFile(dir);
      outputStream = new FileOutputStream(dataFile);
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
  }

  private void initFile(File dir) throws IOException {
    dataFile = new File(dir, FILE_NAME);
    if (dataFile.exists()) {
      loc = dataFile.length();
    } else {
      logger.debug("create new file for id table: " + dir.getName());
      boolean createRes = dataFile.createNewFile();
      if (!createRes) {
        throw new IOException(
            "create new file for id table failed. Path is: " + dataFile.getPath());
      }

      loc = 0;
    }
  }

  @Override
  public long serialize(DiskSchemaEntry schemaEntry) {
    try {
      schemaEntry.serialize(outputStream);
    } catch (IOException e) {
      logger.error("failed to serialize schema entry: " + schemaEntry);
    }
    return 0;
  }

  @TestOnly
  public Collection<DiskSchemaEntry> getAllSchemaEntry() throws IOException {
    FileInputStream inputStream = new FileInputStream(dataFile);
    List<DiskSchemaEntry> res = new ArrayList<>();

    while (true) {
      try {
        DiskSchemaEntry cur = DiskSchemaEntry.deserialize(inputStream);
        res.add(cur);
      } catch (IOException e) {
        logger.debug("read finished");
        break;
      }
    }

    return res;
  }
}
