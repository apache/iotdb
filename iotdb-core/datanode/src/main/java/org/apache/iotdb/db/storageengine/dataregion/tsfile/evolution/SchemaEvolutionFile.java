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

package org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Collection;

/** SchemaEvolutionFile manages schema evolutions related to a TsFileSet. */
public class SchemaEvolutionFile {
  public static final String FILE_SUFFIX = ".sevo";

  private String filePath;

  public SchemaEvolutionFile(String filePath) {
    this.filePath = filePath;
  }

  /**
   * Recover the SchemaEvolutionFile if it is broken.
   *
   * @return true if the file exists false otherwise
   * @throws IOException if the file cannot be recovered
   */
  private boolean recoverFile(boolean forWrite) throws IOException {
    File file = new File(filePath);
    if (!file.exists() || file.length() < Long.BYTES) {
      if (file.exists()) {
        boolean ignored = file.delete();
      }
      if (forWrite) {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "rw")) {
          randomAccessFile.writeLong(8);
        }
      }
      return false;
    }

    long length = file.length();
    long validLength = readValidLength();
    if (validLength == -1) {
      return true;
    }
    if (length > validLength) {
      try (FileOutputStream fis = new FileOutputStream(file, true);
          FileChannel fileChannel = fis.getChannel()) {
        fileChannel.truncate(validLength);
      }
    }
    return true;
  }

  public long readValidLength() throws IOException {
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "r")) {
      return randomAccessFile.readLong();
    }
  }

  public void append(Collection<SchemaEvolution> schemaEvolutions) throws IOException {
    recoverFile(true);

    try (FileOutputStream fos = new FileOutputStream(filePath, true);
        BufferedOutputStream bos = new BufferedOutputStream(fos)) {
      for (SchemaEvolution schemaEvolution : schemaEvolutions) {
        schemaEvolution.serialize(bos);
      }
    }

    File file = new File(filePath);
    long newLength = file.length();
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "rw")) {
      randomAccessFile.writeLong(newLength);
    }
  }

  public EvolvedSchema readAsSchema() throws IOException {
    boolean exists = recoverFile(false);
    if (!exists) {
      return null;
    }

    EvolvedSchema evolvedSchema = new EvolvedSchema();
    try (FileInputStream fis = new FileInputStream(filePath);
        BufferedInputStream bis = new BufferedInputStream(fis)) {
      // skip valid length
      long skipped = bis.skip(8);
      if (skipped != 8) {
        throw new IOException("Cannot skip the length of SchemaEvolutionFile");
      }
      while (bis.available() > 0) {
        SchemaEvolution evolution = SchemaEvolution.createFrom(bis);
        evolution.applyTo(evolvedSchema);
      }
    }
    return evolvedSchema;
  }

  public String getFilePath() {
    return filePath;
  }
}
