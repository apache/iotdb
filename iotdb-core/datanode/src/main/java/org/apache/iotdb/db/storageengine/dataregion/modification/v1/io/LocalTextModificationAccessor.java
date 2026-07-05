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

package org.apache.iotdb.db.storageengine.dataregion.modification.v1.io;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.v1.Modification;

import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
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
  private static final String NO_MODIFICATION_MSG =
      "No modification has been written to this file[{}]";

  private final String filePath;
  private FileOutputStream fos;

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

  // we need to hold the reader for the Iterator, cannot use auto close or close in finally block
  @SuppressWarnings("java:S2095")
  @Override
  public Iterator<Modification> getModificationIterator() {
    File file = FSFactoryProducer.getFSFactory().getFile(filePath);
    final BufferedReader reader;
    try {
      reader = new BufferedReader(new FileReader(file));
    } catch (FileNotFoundException e) {
      logger.debug(NO_MODIFICATION_MSG, file);

      // return empty iterator
      return new Iterator<Modification>() {
        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public Modification next() {
          throw new NoSuchElementException();
        }
      };
    }

    final Modification[] cachedModification = new Modification[1];
    return new Iterator<Modification>() {
      @Override
      public boolean hasNext() {
        try {
          if (cachedModification[0] == null) {
            String line = reader.readLine();
            if (line == null) {
              reader.close();
              return false;
            } else {
              return decodeModificationAndCache(reader, cachedModification, line);
            }
          }
        } catch (IOException e) {
          logger.warn(StorageEngineMessages.ERROR_READING_MODIFICATIONS, e);
        }
        return true;
      }

      @Override
      public Modification next() {
        if (cachedModification[0] == null) {
          throw new NoSuchElementException();
        }
        Modification result = cachedModification[0];
        cachedModification[0] = null;
        return result;
      }
    };
  }

  private boolean decodeModificationAndCache(
      BufferedReader reader, Modification[] cachedModification, String line) throws IOException {
    try {
      cachedModification[0] = decodeModification(line);
      return true;
    } catch (IOException e) {
      logger.warn(StorageEngineMessages.ERROR_DECODE_LINE_TO_MODIFICATION, line);
      cachedModification[0] = null;
      reader.close();
      return false;
    }
  }

  @Override
  public void close() throws IOException {
    if (fos != null) {
      fos.close();
      fos = null;
    }
  }

  @Override
  public void force() throws IOException {
    fos.flush();
    fos.getFD().sync();
  }

  @Override
  public void write(Modification mod) throws IOException {
    writeWithOutSync(mod);
    force();
  }

  @Override
  @SuppressWarnings("java:S2259")
  public void writeWithOutSync(Modification mod) throws IOException {
    if (fos == null) {
      fos = new FileOutputStream(filePath, true);
    }
    fos.write(encodeModification(mod).getBytes());
    fos.write(System.lineSeparator().getBytes());
  }

  @TestOnly
  public void writeInComplete(Modification mod) throws IOException {
    if (fos == null) {
      fos = new FileOutputStream(filePath, true);
    }
    String line = encodeModification(mod);
    if (line != null) {
      fos.write(line.substring(0, 2).getBytes());
      force();
    }
  }

  @TestOnly
  public void writeMeetException(Modification mod) throws IOException {
    if (fos == null) {
      fos = new FileOutputStream(filePath, true);
    }
    writeInComplete(mod);
    throw new IOException();
  }

  @Override
  public void truncate(long size) {
    try (FileOutputStream outputStream =
        new FileOutputStream(FSFactoryProducer.getFSFactory().getFile(filePath), true)) {
      outputStream.getChannel().truncate(size);
      logger.warn(StorageEngineMessages.MODIFICATIONS_WILL_BE_TRUNCATED, filePath, size);
    } catch (FileNotFoundException e) {
      logger.debug(NO_MODIFICATION_MSG, filePath);
    } catch (IOException e) {
      logger.error(
          StorageEngineMessages
              .STORAGE_LOG_AN_ERROR_OCCURRED_WHEN_TRUNCATING_MODIFICATIONS_TO_SIZE_F8A0D6D5,
          filePath,
          size,
          e);
    }
  }

  @Override
  public void mayTruncateLastLine() {
    try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
      long filePointer = file.length() - 1;
      if (filePointer <= 0) {
        return;
      }

      file.seek(filePointer);
      byte lastChar = file.readByte();
      if (lastChar != '\n') {
        while (filePointer > -1 && lastChar != '\n') {
          file.seek(filePointer);
          filePointer--;
          lastChar = file.readByte();
        }
        logger.warn(StorageEngineMessages.LAST_LINE_OF_MODS_INCOMPLETE);
        truncate(filePointer + 2);
      }
    } catch (IOException e) {
      logger.error(StorageEngineMessages.ERROR_READING_MODIFICATIONS, e);
    }
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
    throw new IOException(StorageEngineMessages.UNKNOWN_MODIFICATION_TYPE + fields[0]);
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
      throw new IOException(StorageEngineMessages.INCORRECT_DELETION_FIELDS_NUMBER + fields.length);
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
      throw new IOException(StorageEngineMessages.INVALID_TIMESTAMP + e.getMessage());
    }
    try {
      String[] pathArray = Arrays.copyOfRange(fields, 1, fields.length - 3);
      path = String.join(SEPARATOR, pathArray);
      return new Deletion(new MeasurementPath(path), tsFileOffset, startTimestamp, endTimestamp);
    } catch (IllegalPathException e) {
      throw new IOException(StorageEngineMessages.INVALID_SERIES_PATH + path);
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
      throw new IOException(StorageEngineMessages.INVALID_TIMESTAMP + e.getMessage());
    }
    try {
      String[] pathArray = Arrays.copyOfRange(fields, 1, fields.length - 2);
      path = String.join(SEPARATOR, pathArray);
      return new Deletion(new MeasurementPath(path), versionNum, endTimestamp);
    } catch (IllegalPathException e) {
      throw new IOException(StorageEngineMessages.INVALID_SERIES_PATH + path);
    }
  }
}
