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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PipeTransferTsFileSealWithModReq extends PipeTransferFileSealReqV2 {

  private PipeTransferTsFileSealWithModReq() {
    // Empty constructor
  }

  @Override
  protected PipeRequestType getPlanType() {
    return PipeRequestType.TRANSFER_TS_FILE_SEAL_WITH_MOD;
  }

  private static final String DATABASE_NAME_KEY_PREFIX = "DATABASE_NAME_";
  private static final String WAIT_FOR_SCHEMA_BEFORE_LOAD_KEY = "WAIT_FOR_SCHEMA_BEFORE_LOAD";

  public String getDatabaseNameByTsFileName() {
    return getParameters() == null
        ? null
        : getParameters()
            .get(
                generateDatabaseNameWithFileNameKey(getFileNames().get(getFileNames().size() - 1)));
  }

  public boolean shouldWaitForSchemaBeforeLoad() {
    return getParameters() != null
        && Boolean.parseBoolean(getParameters().get(WAIT_FOR_SCHEMA_BEFORE_LOAD_KEY));
  }

  private static String generateDatabaseNameWithFileNameKey(final String fileName) {
    return DATABASE_NAME_KEY_PREFIX + fileName;
  }

  private static Map<String, String> generateParameters(
      final String tsFileName,
      final String dataBaseName,
      final boolean shouldWaitForSchemaBeforeLoad) {
    final Map<String, String> parameters = new HashMap<>();
    if (dataBaseName != null) {
      parameters.put(generateDatabaseNameWithFileNameKey(tsFileName), dataBaseName);
    }
    if (shouldWaitForSchemaBeforeLoad) {
      parameters.put(WAIT_FOR_SCHEMA_BEFORE_LOAD_KEY, Boolean.TRUE.toString());
    }
    return parameters;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferTsFileSealWithModReq toTPipeTransferReq(
      String modFileName, long modFileLength, String tsFileName, long tsFileLength)
      throws IOException {
    return toTPipeTransferReq(modFileName, modFileLength, tsFileName, tsFileLength, null, false);
  }

  public static PipeTransferTsFileSealWithModReq toTPipeTransferReq(
      final String modFileName,
      final long modFileLength,
      final String tsFileName,
      final long tsFileLength,
      final String dataBaseName)
      throws IOException {
    return toTPipeTransferReq(
        modFileName, modFileLength, tsFileName, tsFileLength, dataBaseName, false);
  }

  public static PipeTransferTsFileSealWithModReq toTPipeTransferReq(
      final String modFileName,
      final long modFileLength,
      final String tsFileName,
      final long tsFileLength,
      final String dataBaseName,
      final boolean shouldWaitForSchemaBeforeLoad)
      throws IOException {
    return (PipeTransferTsFileSealWithModReq)
        new PipeTransferTsFileSealWithModReq()
            .convertToTPipeTransferReq(
                Arrays.asList(modFileName, tsFileName),
                Arrays.asList(modFileLength, tsFileLength),
                generateParameters(tsFileName, dataBaseName, shouldWaitForSchemaBeforeLoad));
  }

  public static PipeTransferTsFileSealWithModReq toTPipeTransferReq(
      final String tsFileName, final long tsFileLength, final String dataBaseName)
      throws IOException {
    return toTPipeTransferReq(tsFileName, tsFileLength, dataBaseName, false);
  }

  public static PipeTransferTsFileSealWithModReq toTPipeTransferReq(
      final String tsFileName,
      final long tsFileLength,
      final String dataBaseName,
      final boolean shouldWaitForSchemaBeforeLoad)
      throws IOException {
    return (PipeTransferTsFileSealWithModReq)
        new PipeTransferTsFileSealWithModReq()
            .convertToTPipeTransferReq(
                Collections.singletonList(tsFileName),
                Collections.singletonList(tsFileLength),
                generateParameters(tsFileName, dataBaseName, shouldWaitForSchemaBeforeLoad));
  }

  public static PipeTransferTsFileSealWithModReq fromTPipeTransferReq(TPipeTransferReq req) {
    return (PipeTransferTsFileSealWithModReq)
        new PipeTransferTsFileSealWithModReq().translateFromTPipeTransferReq(req);
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(
      String modFileName, long modFileLength, String tsFileName, long tsFileLength)
      throws IOException {
    return toTPipeTransferBytes(modFileName, modFileLength, tsFileName, tsFileLength, null, false);
  }

  public static byte[] toTPipeTransferBytes(
      final String modFileName,
      final long modFileLength,
      final String tsFileName,
      final long tsFileLength,
      final String dataBaseName)
      throws IOException {
    return toTPipeTransferBytes(
        modFileName, modFileLength, tsFileName, tsFileLength, dataBaseName, false);
  }

  public static byte[] toTPipeTransferBytes(
      final String modFileName,
      final long modFileLength,
      final String tsFileName,
      final long tsFileLength,
      final String dataBaseName,
      final boolean shouldWaitForSchemaBeforeLoad)
      throws IOException {
    return new PipeTransferTsFileSealWithModReq()
        .convertToTPipeTransferSnapshotSealBytes(
            Arrays.asList(modFileName, tsFileName),
            Arrays.asList(modFileLength, tsFileLength),
            generateParameters(tsFileName, dataBaseName, shouldWaitForSchemaBeforeLoad));
  }

  public static byte[] toTPipeTransferBytes(
      final String tsFileName, final long tsFileLength, final String dataBaseName)
      throws IOException {
    return toTPipeTransferBytes(tsFileName, tsFileLength, dataBaseName, false);
  }

  public static byte[] toTPipeTransferBytes(
      final String tsFileName,
      final long tsFileLength,
      final String dataBaseName,
      final boolean shouldWaitForSchemaBeforeLoad)
      throws IOException {
    return new PipeTransferTsFileSealWithModReq()
        .convertToTPipeTransferSnapshotSealBytes(
            Collections.singletonList(tsFileName),
            Collections.singletonList(tsFileLength),
            generateParameters(tsFileName, dataBaseName, shouldWaitForSchemaBeforeLoad));
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    return obj instanceof PipeTransferTsFileSealWithModReq && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
