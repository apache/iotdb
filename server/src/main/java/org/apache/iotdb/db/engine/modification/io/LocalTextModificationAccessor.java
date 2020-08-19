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

package org.apache.iotdb.db.engine.modification.io;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LocalTextModificationAccessor uses a file on local file system to store the modifications
 * in text format, and writes modifications by appending to the tail of the file.
 */
public class LocalTextModificationAccessor implements ModificationReader, ModificationWriter, AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(LocalTextModificationAccessor.class);
  private static final String SEPARATOR = ",";
  private static final String ABORT_MARK = "aborted";

  private String filePath;
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
    if (!FSFactoryProducer.getFSFactory().getFile(filePath).exists()) {
      logger.debug("No modification has been written to this file");
      return new ArrayList<>();
    }

    String line;
    List<Modification> modificationList = new ArrayList<>();
    try(BufferedReader reader = FSFactoryProducer.getFSFactory().getBufferedReader(filePath)) {
      while ((line = reader.readLine()) != null) {
        if (line.equals(ABORT_MARK) && !modificationList.isEmpty()) {
          modificationList.remove(modificationList.size() - 1);
        } else {
          modificationList.add(decodeModification(line));
        }
      }
    } catch (IOException e) {
      logger.error("An error occurred when reading modifications, and the remaining modifications "
          + "were ignored.", e);
    }
    return modificationList;
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
    if (mod instanceof Deletion)
      return encodeDeletion((Deletion) mod);
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
    return del.getType().toString() + SEPARATOR + del.getPathString()
        + SEPARATOR + del.getVersionNum() + SEPARATOR
        + del.getStartTime() + SEPARATOR + del.getEndTime();
  }

  private static Deletion decodeDeletion(String[] fields) throws IOException {
    if (fields.length != 5 && fields.length != 4) {
      throw new IOException("Incorrect deletion fields number: " + fields.length);
    }

    String path = fields[1];
    long versionNum;
    long startTimestamp = Long.MIN_VALUE;
    long endTimestamp;
    try {
      versionNum = Long.parseLong(fields[2]);
    } catch (NumberFormatException e) {
      throw new IOException("Invalid version number: " + fields[2]);
    }

    try {
      if (fields.length == 4) {
        endTimestamp = Long.parseLong(fields[3]);

      } else {
        startTimestamp = Long.parseLong(fields[3]);
        endTimestamp = Long.parseLong(fields[4]);
      }
      return new Deletion(new PartialPath(path), versionNum, startTimestamp, endTimestamp);
    } catch (NumberFormatException | IllegalPathException e) {
      throw new IOException("Invalid timestamp: " + e.getMessage());
    }

  }
}
