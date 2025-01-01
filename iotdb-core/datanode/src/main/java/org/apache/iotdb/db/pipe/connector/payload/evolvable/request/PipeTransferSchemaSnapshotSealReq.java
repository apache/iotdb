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

package org.apache.iotdb.db.pipe.connector.payload.evolvable.request;

import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferFileSealReqV2;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PipeTransferSchemaSnapshotSealReq extends PipeTransferFileSealReqV2 {

  private PipeTransferSchemaSnapshotSealReq() {
    // Empty constructor
  }

  @Override
  protected PipeRequestType getPlanType() {
    return PipeRequestType.TRANSFER_SCHEMA_SNAPSHOT_SEAL;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferSchemaSnapshotSealReq toTPipeTransferReq(
      final String pattern,
      final String mTreeSnapshotName,
      final long mTreeSnapshotLength,
      final String tLogName,
      final long tLogLength,
      final String databaseName,
      final String typeString)
      throws IOException {
    final Map<String, String> parameters = new HashMap<>();
    parameters.put(ColumnHeaderConstant.PATH_PATTERN, pattern);
    parameters.put(ColumnHeaderConstant.DATABASE, databaseName);
    parameters.put(ColumnHeaderConstant.TYPE, typeString);
    return (PipeTransferSchemaSnapshotSealReq)
        new PipeTransferSchemaSnapshotSealReq()
            .convertToTPipeTransferReq(
                Objects.nonNull(tLogName)
                    ? Arrays.asList(mTreeSnapshotName, tLogName)
                    : Collections.singletonList(mTreeSnapshotName),
                Objects.nonNull(tLogName)
                    ? Arrays.asList(mTreeSnapshotLength, tLogLength)
                    : Collections.singletonList(mTreeSnapshotLength),
                parameters);
  }

  public static PipeTransferSchemaSnapshotSealReq fromTPipeTransferReq(final TPipeTransferReq req) {
    return (PipeTransferSchemaSnapshotSealReq)
        new PipeTransferSchemaSnapshotSealReq().translateFromTPipeTransferReq(req);
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(
      final String pattern,
      final String mTreeSnapshotName,
      final long mTreeSnapshotLength,
      final String tLogName,
      final long tLogLength,
      final String databaseName,
      final String typeString)
      throws IOException {
    final Map<String, String> parameters = new HashMap<>();
    parameters.put(ColumnHeaderConstant.PATH_PATTERN, pattern);
    parameters.put(ColumnHeaderConstant.DATABASE, databaseName);
    parameters.put(ColumnHeaderConstant.TYPE, typeString);
    return new PipeTransferSchemaSnapshotSealReq()
        .convertToTPipeTransferSnapshotSealBytes(
            Objects.nonNull(tLogName)
                ? Arrays.asList(mTreeSnapshotName, tLogName)
                : Collections.singletonList(mTreeSnapshotName),
            Objects.nonNull(tLogName)
                ? Arrays.asList(mTreeSnapshotLength, tLogLength)
                : Collections.singletonList(mTreeSnapshotLength),
            parameters);
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof PipeTransferSchemaSnapshotSealReq && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
