/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.engine.modification.io;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LocalTextModificationAccessor uses a file on local file system to store the modifications
 * in text format, and writes modifications by appending to the tail of the file.
 */
public class LocalTextModificationAccessor implements ModificationReader, ModificationWriter {

  private static final Logger logger = LoggerFactory.getLogger(LocalTextModificationAccessor.class);
  private static final String SEPARATOR = ",";

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
  public Collection<Modification> read() throws IOException {
    BufferedReader reader;
    try {
      reader = new BufferedReader(new FileReader(filePath));
    } catch (FileNotFoundException e) {
      return null;
    }
    String line;

    List<Modification> modificationList = new ArrayList<>();
    try {
      while ((line = reader.readLine()) != null) {
        modificationList.add(decodeModification(line));
      }
    } catch (IOException e) {
      reader.close();
      logger.error("An error occurred when reading modifications, and the remaining modifications "
              + "were ignored.", e);
    }
    return modificationList;
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.close();
    }
  }

  @Override
  public void write(Modification mod) throws IOException {
    if (writer == null) {
      writer = new BufferedWriter(new FileWriter(filePath, true));
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
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(del.getType().toString()).append(SEPARATOR).append(del.getPath())
            .append(SEPARATOR).append(del.getVersionNum()).append(SEPARATOR)
            .append(del.getTimestamp());
    return stringBuilder.toString();
  }

  private static Deletion decodeDeletion(String[] fields) throws IOException {
    if (fields.length != 4) {
      throw new IOException("Incorrect deletion fields number: " + fields.length);
    }

    String path = fields[1];
    long versionNum, timestamp;
    try {
      versionNum = Long.parseLong(fields[2]);
    } catch (NumberFormatException e) {
      throw new IOException("Invalide version number: " + fields[2]);
    }
    try {
      timestamp = Long.parseLong(fields[3]);
    } catch (NumberFormatException e) {
      throw new IOException("Invalide timestamp: " + fields[3]);
    }

    return new Deletion(path, versionNum, timestamp);
  }
}
