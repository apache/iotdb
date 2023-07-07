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

package org.apache.iotdb.db.storageengine.dataregion.modification.io;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.modification.utils.TracedBufferedReader;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * LocalTextModificationAccessor uses a file on local file system to store the modifications in text
 * format, and writes modifications by appending to the tail of the file.
 */
public class LocalTextModificationAccessor
    implements ModificationReader, ModificationWriter, AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(LocalTextModificationAccessor.class);
  private static final String SEPARATOR = ",";
  private static final String ABORT_MARK = "aborted";
  private static final String NO_MODIFICATION_MSG = "No modification has been written to this file";
  private static final String TRUNCATE_MODIFICATION_MSG =
      "An error occurred when reading modifications, and the remaining modifications will be truncated to size {}.";

  private final String filePath;
  private BufferedWriter writer;

  /**
   * Construct a LocalTextModificationAccessor using a file specified by filePath.
   *
   * @param filePath the path of the file that is used for storing modifications.
   */
  public LocalTextModificationAccessor(String filePath) {
    this.filePath = filePath;
  }

  @Override
  public Collection<Modification> read() {
    List<Modification> result = new ArrayList<>();
    Iterator<Modification> iterator = getModificationIterator();
    while (iterator.hasNext()) {
      result.add(iterator.next());
    }
    return result;
  }

  @Override
  public Iterator<Modification> getModificationIterator() {
    File file = FSFactoryProducer.getFSFactory().getFile(filePath);
    // we need to cache two lines to ensure because of ABORT case
    final String[] cachedLines = new String[2];
    final Modification[] cachedModification = new Modification[1];
    final TracedBufferedReader reader;
    final long[] truncatedSize = {0};
    final boolean[] isInit = new boolean[] {true};
    final boolean[] isTruncated = new boolean[1];

    try {
      reader = new TracedBufferedReader(new FileReader(file));
    } catch (FileNotFoundException e) {
      logger.debug(NO_MODIFICATION_MSG);

      // return empty iterator
      return new Iterator<Modification>() {
        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public Modification next() {
          try {
            return null;
          } catch (Exception e) {
            // just for passing Sonar
            throw new NoSuchElementException();
          }
        }
      };
    }
    return new Iterator<Modification>() {
      @Override
      public boolean hasNext() {
        try {
          if (isTruncated[0]) {
            return false;
          }

          truncatedSize[0] = reader.position();
          boolean hasNext = true;
          if (cachedLines[1] == null && cachedLines[0] == null) {
            // we need to read two lines when init,
            // otherwise means there are no more lines
            if (isInit[0]) {
              hasNext = (cachedLines[0] = reader.readLine()) != null;

              // the first line may be ABORT when init
              if (ABORT_MARK.equals(cachedLines[0])) {
                cachedLines[0] = null;
                isInit[0] = true;
                return hasNext();
              }

              if (hasNext) {
                cachedLines[1] = reader.readLine();
              }
              isInit[0] = false;
            } else {
              return false;
            }
          } else if (cachedLines[1] == null) {
            cachedLines[1] = reader.readLine();
          }

          // meet ABORT line, the last line also need to be discarded
          if (ABORT_MARK.equals(cachedLines[1])) {
            cachedLines[0] = null;
            cachedLines[1] = null;
            isInit[0] = true;
            return hasNext();
          }

          if (!hasNext) {
            reader.close();
            return false;
          }
          cachedModification[0] = decodeModification(cachedLines[0]);
          return true;
        } catch (IOException e) {
          try {
            reader.close();
          } catch (IOException ex) {
            logger.warn("Fail to close reader", ex);
          }
          logger.error(TRUNCATE_MODIFICATION_MSG, 0, e);
          truncateModFile(file, cachedLines[1] == null ? truncatedSize[0] : truncatedSize[0]);
        }

        return false;
      }

      @Override
      public Modification next() {
        if (cachedModification[0] == null) {
          throw new NoSuchElementException();
        } else {
          cachedLines[0] = cachedLines[1];
          cachedLines[1] = null;
        }

        return cachedModification[0];
      }
    };
  }

  private void truncateModFile(File file, long truncatedSize) {
    try (FileOutputStream outputStream = new FileOutputStream(file, true)) {
      outputStream.getChannel().truncate(truncatedSize);
    } catch (FileNotFoundException e) {
      logger.debug(NO_MODIFICATION_MSG);
    } catch (IOException e) {
      logger.error("An error occurred when truncating modifications to size {}.", truncatedSize, e);
    }
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.close();
      writer = null;
    }
  }

  @Override
  public void abort() throws IOException {
    if (writer == null) {
      writer = FSFactoryProducer.getFSFactory().getBufferedWriter(filePath, true);
    }
    writer.write(ABORT_MARK);
    writer.newLine();
    writer.flush();
  }

  @Override
  public void write(Modification mod) throws IOException {
    if (writer == null) {
      writer = FSFactoryProducer.getFSFactory().getBufferedWriter(filePath, true);
    }
    writer.write(encodeModification(mod));
    writer.newLine();
    writer.flush();
  }

  private static String encodeModification(Modification mod) {
    if (mod instanceof Deletion) {
      return encodeDeletion((Deletion) mod);
    }
    return null;
  }

  private static Modification decodeModification(String src) throws IOException {
    String[] fields = src.split(SEPARATOR);
    if (Modification.Type.DELETION.name().equals(fields[0])) {
      return decodeDeletion(fields);
    }
    throw new IOException("Unknown modification type: " + fields[0]);
  }

  private static String encodeDeletion(Deletion del) {
    return del.getType()
        + SEPARATOR
        + del.getPathString()
        + SEPARATOR
        + del.getFileOffset()
        + SEPARATOR
        + del.getStartTime()
        + SEPARATOR
        + del.getEndTime();
  }

  /**
   * Decode a range deletion record. E.g. "DELETION,root.ln.wf01.wt01.temperature,111,100,300" the
   * index of field endTimestamp is length - 1, startTimestamp is length - 2, TsFile offset is
   * length - 3. Fields in index range [1, length -3) all belong to a timeseries path in case when
   * the path contains comma.
   *
   * @throws IOException if there is invalid timestamp.
   */
  private static Deletion decodeDeletion(String[] fields) throws IOException {
    if (fields.length < 4) {
      throw new IOException("Incorrect deletion fields number: " + fields.length);
    }

    String path = "";
    long startTimestamp;
    long endTimestamp;
    long tsFileOffset;
    try {
      tsFileOffset = Long.parseLong(fields[fields.length - 3]);
    } catch (NumberFormatException e) {
      return decodePointDeletion(fields);
    }

    try {
      endTimestamp = Long.parseLong(fields[fields.length - 1]);
      startTimestamp = Long.parseLong(fields[fields.length - 2]);
    } catch (NumberFormatException e) {
      throw new IOException("Invalid timestamp: " + e.getMessage());
    }
    try {
      String[] pathArray = Arrays.copyOfRange(fields, 1, fields.length - 3);
      path = String.join(SEPARATOR, pathArray);
      return new Deletion(new PartialPath(path), tsFileOffset, startTimestamp, endTimestamp);
    } catch (IllegalPathException e) {
      throw new IOException("Invalid series path: " + path);
    }
  }

  /**
   * Decode a point deletion record. E.g. "DELETION,root.ln.wf01.wt01.temperature,111,300" the index
   * of field endTimestamp is length - 1, versionNum is length - 2. Fields in index range [1, length
   * - 2) compose timeseries path.
   *
   * @throws IOException if there is invalid timestamp.
   */
  private static Deletion decodePointDeletion(String[] fields) throws IOException {
    String path = "";
    long versionNum;
    long endTimestamp;
    try {
      endTimestamp = Long.parseLong(fields[fields.length - 1]);
      versionNum = Long.parseLong(fields[fields.length - 2]);
    } catch (NumberFormatException e) {
      throw new IOException("Invalid timestamp: " + e.getMessage());
    }
    try {
      String[] pathArray = Arrays.copyOfRange(fields, 1, fields.length - 2);
      path = String.join(SEPARATOR, pathArray);
      return new Deletion(new PartialPath(path), versionNum, endTimestamp);
    } catch (IllegalPathException e) {
      throw new IOException("Invalid series path: " + path);
    }
  }
}
