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

package org.apache.iotdb.db.pipe.sink.payload.evolvable.request;

import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFileSealReqV2;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class PipeTransferMultiFileSealReq extends PipeTransferFileSealReqV2 {

  private PipeTransferMultiFileSealReq() {
    // Empty constructor
  }

  @Override
  protected PipeRequestType getPlanType() {
    return PipeRequestType.TRANSFER_TS_FILE_SEAL_WITH_MOD;
  }

  protected static final String DATABASE_NAME_KEY_PREFIX = "DATABASE_NAME_";

  public String getDatabaseNameByTsFileName() {
    return parameters == null
        ? null
        : parameters.get(generateDatabaseNameWithFileNameKey(fileNames.get(fileNames.size() - 1)));
  }

  protected static String generateDatabaseNameWithFileNameKey(final String fileName) {
    return DATABASE_NAME_KEY_PREFIX + fileName;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferMultiFileSealReq toTPipeTransferReq(
      final String modFileName,
      final long modFileLength,
      final String tsFileName,
      final long tsFileLength)
      throws IOException {
    // Sevo file depending on incoming adaptations
    return toTPipeTransferReq(modFileName, modFileLength, null, 0L, tsFileName, tsFileLength, null);
  }

  public static PipeTransferMultiFileSealReq toTPipeTransferReq(
      final String modFileName,
      final long modFileLength,
      final String sevoFileName,
      final long sevoFileLength,
      final String tsFileName,
      final long tsFileLength,
      final String dataBaseName)
      throws IOException {
    final List<String> fileNames = new ArrayList<>();
    final List<Long> fileLengths = new ArrayList<>();
    if (Objects.nonNull(modFileName)) {
      fileNames.add(modFileName);
      fileLengths.add(modFileLength);
    }
    if (Objects.nonNull(sevoFileName)) {
      fileNames.add(sevoFileName);
      fileLengths.add(sevoFileLength);
    }
    fileNames.add(tsFileName);
    fileLengths.add(tsFileLength);
    return (PipeTransferMultiFileSealReq)
        new PipeTransferMultiFileSealReq()
            .convertToTPipeTransferReq(
                fileNames,
                fileLengths,
                Collections.singletonMap(
                    generateDatabaseNameWithFileNameKey(tsFileName), dataBaseName));
  }

  public static PipeTransferMultiFileSealReq fromTPipeTransferReq(final TPipeTransferReq req) {
    return (PipeTransferMultiFileSealReq)
        new PipeTransferMultiFileSealReq().translateFromTPipeTransferReq(req);
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(
      final String modFileName,
      final long modFileLength,
      final String sevoFileName,
      final long sevoFileLength,
      final String tsFileName,
      final long tsFileLength,
      final String dataBaseName)
      throws IOException {
    final List<String> fileNames = new ArrayList<>();
    final List<Long> fileLengths = new ArrayList<>();
    if (Objects.nonNull(modFileName)) {
      fileNames.add(modFileName);
      fileLengths.add(modFileLength);
    }
    if (Objects.nonNull(sevoFileName)) {
      fileNames.add(sevoFileName);
      fileLengths.add(sevoFileLength);
    }
    fileNames.add(tsFileName);
    fileLengths.add(tsFileLength);
    return new PipeTransferMultiFileSealReq()
        .convertToTPipeTransferSnapshotSealBytes(
            fileNames,
            fileLengths,
            Collections.singletonMap(
                generateDatabaseNameWithFileNameKey(tsFileName), dataBaseName));
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof PipeTransferMultiFileSealReq && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
