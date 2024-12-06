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

package org.apache.iotdb.confignode.manager.pipe.connector.payload;

import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferFileSealReqV2;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.persistence.schema.CNSnapshotFileType;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PipeTransferConfigSnapshotSealReq extends PipeTransferFileSealReqV2 {

  public static final String FILE_TYPE = "fileType";

  private PipeTransferConfigSnapshotSealReq() {
    // Empty constructor
  }

  @Override
  protected PipeRequestType getPlanType() {
    return PipeRequestType.TRANSFER_CONFIG_SNAPSHOT_SEAL;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferConfigSnapshotSealReq toTPipeTransferReq(
      final String pattern,
      final String snapshotName,
      final long snapshotLength,
      final String templateFileName,
      final long templateFileLength,
      final CNSnapshotFileType fileType,
      final String typeString)
      throws IOException {
    final Map<String, String> parameters = new HashMap<>();
    parameters.put(ColumnHeaderConstant.PATH_PATTERN, pattern);
    parameters.put(FILE_TYPE, Byte.toString(fileType.getType()));
    parameters.put(ColumnHeaderConstant.TYPE, typeString);

    return (PipeTransferConfigSnapshotSealReq)
        new PipeTransferConfigSnapshotSealReq()
            .convertToTPipeTransferReq(
                Objects.nonNull(templateFileName)
                    ? Arrays.asList(snapshotName, templateFileName)
                    : Collections.singletonList(snapshotName),
                Objects.nonNull(templateFileName)
                    ? Arrays.asList(snapshotLength, templateFileLength)
                    : Collections.singletonList(snapshotLength),
                parameters);
  }

  public static PipeTransferConfigSnapshotSealReq fromTPipeTransferReq(final TPipeTransferReq req) {
    return (PipeTransferConfigSnapshotSealReq)
        new PipeTransferConfigSnapshotSealReq().translateFromTPipeTransferReq(req);
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(
      final String pattern,
      final String snapshotName,
      final long snapshotLength,
      final String templateFileName,
      final long templateFileLength,
      final CNSnapshotFileType fileType,
      final String typeString)
      throws IOException {
    final Map<String, String> parameters = new HashMap<>();
    parameters.put(ColumnHeaderConstant.PATH_PATTERN, pattern);
    parameters.put(FILE_TYPE, Byte.toString(fileType.getType()));
    parameters.put(ColumnHeaderConstant.TYPE, typeString);
    return new PipeTransferConfigSnapshotSealReq()
        .convertToTPipeTransferSnapshotSealBytes(
            Objects.nonNull(templateFileName)
                ? Arrays.asList(snapshotName, templateFileName)
                : Collections.singletonList(snapshotName),
            Objects.nonNull(templateFileName)
                ? Arrays.asList(snapshotLength, templateFileLength)
                : Collections.singletonList(snapshotLength),
            parameters);
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof PipeTransferConfigSnapshotSealReq && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
