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

package org.apache.iotdb.db.metadata.idtable;

import org.apache.iotdb.db.metadata.idtable.entry.DiskSchemaEntry;
import org.apache.iotdb.db.utils.TestOnly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** store id table schema in append only file */
public class AppendOnlyDiskSchemaManager implements IDiskSchemaManager {

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
      throw new IllegalArgumentException("can't initialize disk schema manager at " + dataFile);
    }
  }

  private void initFile(File dir) throws IOException {
    // create dirs
    if (dir.mkdirs()) {
      logger.info(
          "ID table create storage group system dir {} doesn't exist, create it",
          dir.getParentFile());
    }

    dataFile = new File(dir, FILE_NAME);
    if (dataFile.exists()) {
      loc = dataFile.length();
      if (!checkLastEntry(loc)) {
        throw new IOException("File corruption");
      }
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

  private boolean checkLastEntry(long pos) {
    // file length is smaller than one int
    if (pos <= Integer.BYTES) {
      return false;
    }

    pos -= Integer.BYTES;
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(dataFile, "r");
        FileInputStream inputStream = new FileInputStream(dataFile)) {
      randomAccessFile.seek(pos);
      int lastEntrySize = randomAccessFile.readInt();
      // last int is not right
      if (pos - lastEntrySize < 0) {
        return false;
      }

      long realSkip = inputStream.skip(pos - lastEntrySize);
      // file length isn't right
      if (realSkip != pos - lastEntrySize) {
        return false;
      }

      DiskSchemaEntry.deserialize(inputStream);
    } catch (Exception e) {
      logger.error("can't deserialize last entry, file corruption." + e);
      return false;
    }

    return true;
  }

  @Override
  public long serialize(DiskSchemaEntry schemaEntry) {
    try {
      schemaEntry.serialize(outputStream);
    } catch (IOException e) {
      logger.error("failed to serialize schema entry: " + schemaEntry);
      throw new IllegalArgumentException("can't serialize disk entry of " + schemaEntry);
    }

    return 0;
  }

  @TestOnly
  public Collection<DiskSchemaEntry> getAllSchemaEntry() throws IOException {
    FileInputStream inputStream = new FileInputStream(dataFile);
    List<DiskSchemaEntry> res = new ArrayList<>();
    // for test, we read at most 1000 entries.
    int maxCount = 1000;

    while (maxCount > 0) {
      try {
        maxCount--;
        DiskSchemaEntry cur = DiskSchemaEntry.deserialize(inputStream);
        res.add(cur);
      } catch (IOException e) {
        logger.debug("read finished");
        break;
      }
    }

    // free resource
    inputStream.close();

    return res;
  }

  @Override
  public void close() throws IOException {
    try {
      outputStream.close();
    } catch (IOException e) {
      logger.error("close schema file failed");
      throw e;
    }
  }
}
