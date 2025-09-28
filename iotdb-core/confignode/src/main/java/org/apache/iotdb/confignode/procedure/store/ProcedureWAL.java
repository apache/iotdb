1/*
1 * Licensed to the Apache Software Foundation (ASF) under one
1 * or more contributor license agreements.  See the NOTICE file
1 * distributed with this work for additional information
1 * regarding copyright ownership.  The ASF licenses this file
1 * to you under the Apache License, Version 2.0 (the
1 * "License"); you may not use this file except in compliance
1 * with the License.  You may obtain a copy of the License at
1 *
1 *     http://www.apache.org/licenses/LICENSE-2.0
1 *
1 * Unless required by applicable law or agreed to in writing,
1 * software distributed under the License is distributed on an
1 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1 * KIND, either express or implied.  See the License for the
1 * specific language governing permissions and limitations
1 * under the License.
1 */
1
1package org.apache.iotdb.confignode.procedure.store;
1
1import org.apache.iotdb.commons.utils.TestOnly;
1import org.apache.iotdb.confignode.procedure.Procedure;
1
1import org.apache.tsfile.utils.PublicBAOS;
1import org.slf4j.Logger;
1import org.slf4j.LoggerFactory;
1
1import java.io.DataOutputStream;
1import java.io.File;
1import java.io.FileOutputStream;
1import java.io.IOException;
1import java.nio.ByteBuffer;
1import java.nio.channels.FileChannel;
1import java.nio.file.Files;
1import java.nio.file.Path;
1
1/** Reserve this class for version upgrade test. */
1@TestOnly
1public class ProcedureWAL {
1
1  private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureWAL.class);
1
1  private IProcedureFactory procedureFactory;
1  private Path walFilePath;
1
1  public ProcedureWAL(Path walFilePath, IProcedureFactory procedureFactory) {
1    this.walFilePath = walFilePath;
1    this.procedureFactory = procedureFactory;
1  }
1
1  /**
1   * Create a wal file
1   *
1   * @throws IOException ioe
1   */
1  @TestOnly
1  public void save(Procedure procedure) throws IOException {
1    File walTmp = new File(walFilePath + ".tmp");
1    Path walTmpPath = walTmp.toPath();
1    Files.deleteIfExists(walTmpPath);
1    Files.createFile(walTmpPath);
1    try (FileOutputStream fos = new FileOutputStream(walTmp);
1        FileChannel channel = fos.getChannel();
1        PublicBAOS publicBAOS = new PublicBAOS();
1        DataOutputStream dataOutputStream = new DataOutputStream(publicBAOS)) {
1      procedure.serialize(dataOutputStream);
1      channel.write(ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size()));
1
1      channel.force(true);
1      fos.getFD().sync();
1    }
1    Files.deleteIfExists(walFilePath);
1    Files.move(walTmpPath, walFilePath);
1  }
1}
1