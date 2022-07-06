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

package org.apache.iotdb.confignode.procedure.store;

import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class ProcedureWAL {

  private static final Logger LOG = LoggerFactory.getLogger(ProcedureWAL.class);

  private static final String TMP_SUFFIX = ".tmp";
  private static final int PROCEDURE_WAL_BUFFER_SIZE = 8 * 1024 * 1024;
  private IProcedureFactory procedureFactory;
  private Path walFilePath;

  public ProcedureWAL(Path walFilePath, IProcedureFactory procedureFactory) {
    this.walFilePath = walFilePath;
    this.procedureFactory = procedureFactory;
  }

  /**
   * Create a wal file
   *
   * @throws IOException ioe
   */
  public void save(Procedure procedure) throws IOException {
    File walTmp = new File(walFilePath + TMP_SUFFIX);
    Path walTmpPath = walTmp.toPath();
    Files.deleteIfExists(walTmpPath);
    Files.createFile(walTmpPath);
    try (FileOutputStream fos = new FileOutputStream(walTmp);
        FileChannel channel = fos.getChannel();
        PublicBAOS publicBAOS = new PublicBAOS();
        DataOutputStream dataOutputStream = new DataOutputStream(publicBAOS)) {
      procedure.serialize(dataOutputStream);
      channel.write(ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size()));
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
    Procedure procedure = null;
    try (FileInputStream fis = new FileInputStream(walFilePath.toFile());
        FileChannel channel = fis.getChannel()) {
      ByteBuffer byteBuffer = ByteBuffer.allocate(PROCEDURE_WAL_BUFFER_SIZE);
      if (channel.read(byteBuffer) > 0) {
        byteBuffer.flip();
        procedure = procedureFactory.create(byteBuffer);
        byteBuffer.clear();
      }
      procedureList.add(procedure);
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
