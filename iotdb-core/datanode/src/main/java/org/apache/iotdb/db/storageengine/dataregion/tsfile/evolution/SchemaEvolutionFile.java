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

import org.apache.iotdb.commons.utils.FileUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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
  private boolean recoverFile() throws IOException {
    File file = new File(filePath);
    if (!file.exists() || file.length() == 0) {
      return false;
    }

    long length = file.length();
    String fileName = file.getName();
    long validLength = parseValidLength(fileName);
    if (length > validLength) {
      try (FileOutputStream fis = new FileOutputStream(file, true);
          FileChannel fileChannel = fis.getChannel()) {
        fileChannel.truncate(validLength);
      }
    }
    return true;
  }

  public static long parseValidLength(String fileName) {
    return Long.parseLong(fileName.substring(0, fileName.lastIndexOf('.')));
  }

  public void append(Collection<SchemaEvolution> schemaEvolutions) throws IOException {
    recoverFile();

    try (FileOutputStream fos = new FileOutputStream(filePath, true);
        BufferedOutputStream bos = new BufferedOutputStream(fos)) {
      for (SchemaEvolution schemaEvolution : schemaEvolutions) {
        schemaEvolution.serialize(bos);
      }
    }

    File originFile = new File(filePath);
    long newLength = originFile.length();
    File newFile = new File(originFile.getParentFile(), newLength + FILE_SUFFIX);
    FileUtils.moveFileSafe(originFile, newFile);
    filePath = newFile.getAbsolutePath();
  }

  public EvolvedSchema readAsSchema() throws IOException {
    boolean exists = recoverFile();
    if (!exists) {
      return null;
    }

    EvolvedSchema evolvedSchema = new EvolvedSchema();
    try (FileInputStream fis = new FileInputStream(filePath);
        BufferedInputStream bis = new BufferedInputStream(fis)) {
      while (bis.available() > 0) {
        SchemaEvolution evolution = SchemaEvolution.createFrom(bis);
        evolution.applyTo(evolvedSchema);
      }
    }
    return evolvedSchema;
  }
}
