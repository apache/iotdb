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

import org.apache.iotdb.commons.utils.TestOnly;
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
import java.util.Optional;

public class ProcedureWAL {

  private static final Logger LOG = LoggerFactory.getLogger(ProcedureWAL.class);
  private static final int PROCEDURE_WAL_BUFFER_SIZE = 8 * 1024 * 1024;

  /** Load wal files into memory */
  public static Optional<Procedure> load(Path walFilePath, IProcedureFactory procedureFactory) {
    try (FileInputStream fis = new FileInputStream(walFilePath.toFile())) {
      return load(fis, procedureFactory);
    } catch (IOException e) {
      LOG.error("Load {} failed, it will be deleted.", walFilePath, e);
      if (!walFilePath.toFile().delete()) {
        LOG.error("{} delete failed; take appropriate action.", walFilePath, e);
      }
    }
    return Optional.empty();
  }

  public static Optional<Procedure> load(
      FileInputStream fileInputStream, IProcedureFactory procedureFactory) throws IOException {
    Procedure procedure = null;
    try (FileChannel channel = fileInputStream.getChannel()) {
      ByteBuffer byteBuffer = ByteBuffer.allocate(PROCEDURE_WAL_BUFFER_SIZE);
      if (channel.read(byteBuffer) > 0) {
        byteBuffer.flip();
        procedure = procedureFactory.create(byteBuffer);
        byteBuffer.clear();
      }
      return Optional.ofNullable(procedure);
    }
  }

  // region TestOnly

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
  @TestOnly
  public void save(Procedure procedure) throws IOException {
    File walTmp = new File(walFilePath + ".tmp");
    Path walTmpPath = walTmp.toPath();
    Files.deleteIfExists(walTmpPath);
    Files.createFile(walTmpPath);
    try (FileOutputStream fos = new FileOutputStream(walTmp);
        FileChannel channel = fos.getChannel();
        PublicBAOS publicBAOS = new PublicBAOS();
        DataOutputStream dataOutputStream = new DataOutputStream(publicBAOS)) {
      procedure.serialize(dataOutputStream);
      channel.write(ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size()));

      channel.force(true);
      fos.getFD().sync();
    }
    Files.deleteIfExists(walFilePath);
    Files.move(walTmpPath, walFilePath);
  }
}
