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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.metadata.idtable.entry.DiskSchemaEntry;
import org.apache.iotdb.db.metadata.idtable.entry.SchemaEntry;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** store id table schema in append only file */
public class AppendOnlyDiskSchemaManager implements IDiskSchemaManager {

  private static final String FILE_NAME = "SeriesKeyMapping.meta";

  // file version to distinguish different id table file
  private static final String FILE_VERSION = "AppendOnly_V1";

  File dataFile;

  FileOutputStream outputStream;

  RandomAccessFile randomAccessFile;

  long loc;

  private static final Logger logger = LoggerFactory.getLogger(AppendOnlyDiskSchemaManager.class);

  public AppendOnlyDiskSchemaManager(File dir) {
    try {
      initFile(dir);
      outputStream = new FileOutputStream(dataFile, true);
      randomAccessFile = new RandomAccessFile(dataFile, "rw");
      // we write file version to new file
      if (loc == 0) {
        ReadWriteIOUtils.write(FILE_VERSION, outputStream);
      }
    } catch (IOException e) {
      logger.error(e.getMessage());
      throw new IllegalArgumentException("can't initialize disk schema manager at " + dataFile);
    }
  }

  private void initFile(File dir) throws IOException {
    // create dirs
    if (dir.mkdirs()) {
      logger.info(
          "ID table create database system dir {} doesn't exist, create it", dir.getParentFile());
    }

    dataFile = new File(dir, FILE_NAME);
    if (dataFile.exists()) {
      loc = dataFile.length();
      if (!checkFileConsistency(loc)) {
        throw new IOException("File corruption");
      }
    } else {
      logger.debug("create new file for id table: {}", dir.getName());
      boolean createRes = dataFile.createNewFile();
      if (!createRes) {
        throw new IOException(
            "create new file for id table failed. Path is: " + dataFile.getPath());
      }

      loc = 0;
    }
  }

  private boolean checkFileConsistency(long pos) {
    // empty file
    if (pos == 0) {
      return true;
    }

    // file length is smaller than one int
    if (pos <= Integer.BYTES) {
      return false;
    }

    try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(dataFile))) {
      // check file version
      String version = ReadWriteIOUtils.readString(inputStream);
      if (!FILE_VERSION.equals(version)) {
        logger.error("File version isn't right, need: {}, actual: {} ", FILE_VERSION, version);
        return false;
      }
    } catch (Exception e) {
      logger.error("File check failed", e);
      return false;
    }

    return true;
  }

  @Override
  public long serialize(DiskSchemaEntry schemaEntry) {
    long beforeLoc = loc;
    try {
      loc += schemaEntry.serialize(outputStream);
    } catch (IOException e) {
      logger.error("failed to serialize schema entry: {}", schemaEntry);
      throw new IllegalArgumentException("can't serialize disk entry of " + schemaEntry);
    }

    return beforeLoc;
  }

  @Override
  public void recover(IDTable idTable) {
    long loc = 0;

    try (FileInputStream inputStream = new FileInputStream(dataFile)) {
      // read file version
      ReadWriteIOUtils.readString(inputStream);

      while (inputStream.available() > 0) {
        DiskSchemaEntry cur = DiskSchemaEntry.deserialize(inputStream);
        if (!cur.deviceID.equals(DiskSchemaEntry.TOMBSTONE)) {
          SchemaEntry schemaEntry =
              new SchemaEntry(
                  TSDataType.deserialize(cur.type),
                  TSEncoding.deserialize(cur.encoding),
                  CompressionType.deserialize(cur.compressor),
                  loc);
          idTable.putSchemaEntry(cur.deviceID, cur.measurementName, schemaEntry, cur.isAligned);
        }
        loc += cur.entrySize;
      }
    } catch (IOException | MetadataException e) {
      logger.info("Last entry is incomplete, we will recover as much as we can.");
      try {
        outputStream.getChannel().truncate(loc);
      } catch (IOException ioException) {
        logger.error("Failed at truncate file.", ioException);
      }
      this.loc = loc;
    }
  }

  @TestOnly
  public Collection<DiskSchemaEntry> getAllSchemaEntry() throws IOException {
    List<DiskSchemaEntry> res = new ArrayList<>();

    try (FileInputStream inputStream = new FileInputStream(dataFile)) {
      // read file version
      ReadWriteIOUtils.readString(inputStream);
      // for test, we read at most 1000 entries.
      int maxCount = 1000;

      while (maxCount > 0) {
        try {
          maxCount--;
          DiskSchemaEntry cur = DiskSchemaEntry.deserialize(inputStream);
          if (!cur.deviceID.equals(DiskSchemaEntry.TOMBSTONE)) {
            res.add(cur);
          }
        } catch (IOException e) {
          logger.debug("read finished");
          break;
        }
      }
    }

    return res;
  }

  /**
   * get DiskSchemaEntries from disk file
   *
   * @param offsets the offset of each record on the disk file
   * @return DiskSchemaEntries
   */
  @Override
  public List<DiskSchemaEntry> getDiskSchemaEntriesByOffset(List<Long> offsets) {
    List<DiskSchemaEntry> diskSchemaEntries = new ArrayList<>(offsets.size());
    Collections.sort(offsets);
    try {
      for (long offset : offsets) {
        diskSchemaEntries.add(getDiskSchemaEntryByOffset(offset));
      }
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
    return diskSchemaEntries;
  }

  /**
   * delete DiskSchemaEntry on disk
   *
   * @param offset the offset of a record on the disk file
   * @throws MetadataException
   */
  @Override
  public void deleteDiskSchemaEntryByOffset(long offset) throws MetadataException {
    try {
      randomAccessFile.seek(offset + FILE_VERSION.length() + Integer.BYTES);
      int strLength = randomAccessFile.readInt();
      byte[] bytes = new byte[strLength];
      // change the deviceID of the DiskSchemaEntry to be deleted to a tombstone: bytes=[0,...,0]
      randomAccessFile.write(bytes, 0, strLength);
    } catch (IOException e) {
      logger.error(e.getMessage());
      throw new MetadataException(e.getMessage());
    }
  }

  private DiskSchemaEntry getDiskSchemaEntryByOffset(long offset) throws IOException {
    randomAccessFile.seek(offset + FILE_VERSION.length() + Integer.BYTES);
    // skip reading deviceID
    readString();
    String seriesKey = readString();
    String measurementName = readString();
    String deviceID =
        DeviceIDFactory.getInstance()
            .getDeviceID(seriesKey.substring(0, seriesKey.length() - measurementName.length() - 1))
            .toStringID();
    return new DiskSchemaEntry(
        deviceID,
        seriesKey,
        measurementName,
        randomAccessFile.readByte(),
        randomAccessFile.readByte(),
        randomAccessFile.readByte(),
        randomAccessFile.readBoolean());
  }

  private String readString() throws IOException {
    int strLength = randomAccessFile.readInt();
    byte[] bytes = new byte[strLength];
    randomAccessFile.read(bytes, 0, strLength);
    return new String(bytes, 0, strLength);
  }

  @Override
  public void close() throws IOException {
    try {
      outputStream.close();
      randomAccessFile.close();
    } catch (IOException e) {
      logger.error("close schema file failed");
      throw e;
    }
  }
}
