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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.reader;

import org.apache.iotdb.lsm.sstable.fileIO.SSTableInputStream;
import org.apache.iotdb.lsm.util.DiskFileNameDescriptor;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * It is used to obtain all matching device ids from the disk file at one time, or iteratively
 * obtain each matching device id.
 */
public class DiskDeviceIDReader implements Iterator<Integer> {
  private static final Logger logger = LoggerFactory.getLogger(DiskDeviceIDReader.class);

  private String flushDirPath;

  // In the process of each iteration, it is necessary to check whether the records obtained by next
  // in the deletionFile have been deleted, and the performance of reading the deletionFile once in
  // each iteration is poor. Considering that there are fewer deletion operations, the deletionFile
  // contains fewer records, which can be read into the set at one time, and the deleted records can
  // be saved in this set.
  private Set<Integer> deletionIDs;

  // If the tifiles to be processed are read iteratively, in order to ensure that the obtained
  // records are in order, the tifiles can be sorted by number of the file name to read
  private final String[] tiFiles;

  /** The result that the {@link #next} function will get. */
  private Integer next;

  private TiFileReader tiFileReader;

  private final Map<String, String> tags;

  // In the process of iteratively obtaining records, the subscript identifying the tifile being
  // read
  private int index;

  public DiskDeviceIDReader(String[] tiFiles, Map<String, String> tags, String flushDirPath) {
    this.tiFiles = tiFiles;
    // in order to ensure that the obtained records are in order, the tifiles can be sorted by
    // number of the file name to read
    Arrays.sort(tiFiles);
    this.tags = tags;
    this.flushDirPath = flushDirPath;
    deletionIDs = new HashSet<>();
    next = null;
    index = 0;
  }

  /**
   * Returns {@code true} if the iteration has more elements. (In other words, returns {@code true}
   * if {@link #next} would return an element rather than throwing an exception.)
   *
   * @return {@code true} if the iteration has more elements
   */
  @Override
  public boolean hasNext() {
    try {
      if (next != null) {
        return true;
      }
      if (tiFiles == null || tiFiles.length == 0) {
        return false;
      }
      if (tiFileReader != null) {
        // If tiFileReader exists, if a record that has not been deleted can be read from this
        // tiFileReader, then return true. Otherwise, the tiFileReader needs to be closed
        while (tiFileReader.hasNext()) {
          next = tiFileReader.next();
          if (!deletionIDs.contains(next)) {
            return true;
          }
        }
        tiFileReader.close();
        tiFileReader = null;
        deletionIDs.clear();
        index++;
      }
      while (index < tiFiles.length) {
        String tiFileName = tiFiles[index];
        tiFileReader = new TiFileReader(new File(flushDirPath + File.separator + tiFileName), tags);
        setDeletionIDs(getDeletionFile(tiFileName));
        while (tiFileReader.hasNext()) {
          next = tiFileReader.next();
          if (!deletionIDs.contains(next)) {
            return true;
          }
        }
        tiFileReader.close();
        tiFileReader = null;
        deletionIDs.clear();
        index++;
      }
    } catch (IOException e) {
      logger.error(e.getMessage());
      try {
        tiFileReader.close();
      } catch (IOException ex) {
        logger.error(ex.getMessage());
      }
    }
    return false;
  }

  /**
   * Returns the next element in the iteration.
   *
   * @return the next element in the iteration
   * @throws NoSuchElementException if the iteration has no more elements
   */
  @Override
  public Integer next() {
    if (next == null) {
      throw new NoSuchElementException();
    }
    int now = next;
    next = null;
    return now;
  }

  /**
   * Get all matching deviceIDs at once
   *
   * @return All matching device ids are saved using RoaringBitmap
   */
  public RoaringBitmap getAllDeviceID() {
    TiFileReader tiFileReader = null;
    RoaringBitmap deviceIDs = null;
    try {
      if (tiFiles == null || tiFiles.length == 0) {
        return new RoaringBitmap();
      }
      int i = 0;
      for (String tiFile : tiFiles) {
        tiFileReader = new TiFileReader(new File(flushDirPath + File.separator + tiFile), tags);
        RoaringBitmap roaringBitmap = tiFileReader.readAllDeviceID();
        if (!roaringBitmap.isEmpty()) {
          File deletionFile = getDeletionFile(tiFile);
          if (deletionFile.exists()) {
            deleteRecords(roaringBitmap, deletionFile);
          }
        }
        if (i == 0) {
          deviceIDs = roaringBitmap;
          i = 1;
        } else {
          deviceIDs.or(roaringBitmap);
        }
        tiFileReader.close();
      }
    } catch (IOException e) {
      logger.error(e.getMessage());
    } finally {
      if (tiFileReader != null) {
        try {
          tiFileReader.close();
        } catch (IOException e) {
          logger.error(e.getMessage());
        }
      }
    }
    return deviceIDs;
  }

  private File getDeletionFile(String flushFileName) {
    return new File(
        flushDirPath
            + File.separator
            + DiskFileNameDescriptor.getFlushDeleteFileNameFromFlushFileName(flushFileName));
  }

  private void setDeletionIDs(File deletionFile) throws IOException {
    SSTableInputStream fileInput = null;
    try {
      if (!deletionFile.exists()) {
        return;
      }
      fileInput = new SSTableInputStream(deletionFile);
      while (true) {
        int id = fileInput.readInt();
        deletionIDs.add(id);
      }
    } catch (EOFException e) {
      logger.info("deletion file {} read end", deletionFile);
    } finally {
      if (fileInput != null) {
        fileInput.close();
      }
    }
  }

  private void deleteRecords(RoaringBitmap roaringBitmap, File deletionFile) throws IOException {
    SSTableInputStream fileInput = null;
    if (roaringBitmap.isEmpty()) return;
    try {
      fileInput = new SSTableInputStream(deletionFile);
      while (true) {
        int id = fileInput.readInt();
        roaringBitmap.remove(id);
      }
    } catch (EOFException e) {
      logger.info("deletion file {} read end", deletionFile);
    } finally {
      if (fileInput != null) {
        fileInput.close();
      }
    }
  }
}
