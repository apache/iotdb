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

package org.apache.iotdb.procedure.store;

import org.apache.iotdb.procedure.Procedure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class ProcedureWAL {

  private static final Logger LOG = LoggerFactory.getLogger(ProcedureWAL.class);

  private static final String TMP_SUFFIX = ".tmp";
  private Path walFilePath;
  private ByteBuffer byteBuffer;

  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  public ProcedureWAL(Path walFilePath, ByteBuffer byteBuffer) {
    this.walFilePath = walFilePath;
    this.byteBuffer = byteBuffer;
  }

  /**
   * Create a wal file
   *
   * @throws IOException ioe
   */
  public void save() throws IOException {
    File walTmp = new File(walFilePath + TMP_SUFFIX);
    Path walTmpPath = walTmp.toPath();
    Files.deleteIfExists(walTmpPath);
    Files.createFile(walTmpPath);
    try (FileOutputStream fos = new FileOutputStream(walTmp);
        FileChannel channel = fos.getChannel()) {
      byteBuffer.flip();
      channel.write(byteBuffer);
    }
    Files.deleteIfExists(walFilePath);
    Files.move(walTmpPath, walFilePath);
  }

  /**
   * Load wal files into memory
   *
   * @param procedureList procedure list
   */
  public void load(List<Procedure> procedureList) {
    if (procedureList == null || procedureList.isEmpty()) {
      LOG.info("None  procedure  wal  found.");
      return;
    }
    Procedure procedure = null;
    try (FileInputStream fis = new FileInputStream(walFilePath.toFile());
        FileChannel channel = fis.getChannel()) {
      while (channel.read(byteBuffer) > 0) {
        byteBuffer.flip();
        while (byteBuffer.hasRemaining()) {
          if (procedure == null) {
            procedure = Procedure.newInstance(byteBuffer);
          } else {
            procedure.deserialize(byteBuffer);
          }
        }
        byteBuffer.clear();
      }
    } catch (IOException e) {
      LOG.error("Load {} failed, it will be deleted.", walFilePath, e);
      walFilePath.toFile().delete();
    }
  }

  public void delete() {
    try {
      Files.deleteIfExists(Paths.get(walFilePath + TMP_SUFFIX));
      Files.deleteIfExists(walFilePath);
    } catch (IOException e) {
      LOG.error("Delete procedure wal failed.");
    }
  }
}
