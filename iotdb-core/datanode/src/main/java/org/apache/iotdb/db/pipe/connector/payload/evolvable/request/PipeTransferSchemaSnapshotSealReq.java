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
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferFileSealReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.io.IOException;

public class PipeTransferSchemaSnapshotSealReq extends PipeTransferFileSealReq {

  private PipeTransferSchemaSnapshotSealReq() {
    // Empty constructor
  }

  @Override
  protected PipeRequestType getPlanType() {
    return PipeRequestType.TRANSFER_SCHEMA_SNAPSHOT_SEAL;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferSchemaSnapshotSealReq toTPipeTransferReq(
      String fileName, long fileLength) throws IOException {
    return (PipeTransferSchemaSnapshotSealReq)
        new PipeTransferSchemaSnapshotSealReq().convertToTPipeTransferReq(fileName, fileLength);
  }

  public static PipeTransferSchemaSnapshotSealReq fromTPipeTransferReq(TPipeTransferReq req) {
    return (PipeTransferSchemaSnapshotSealReq)
        new PipeTransferSchemaSnapshotSealReq().translateFromTPipeTransferReq(req);
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(String fileName, long fileLength) throws IOException {
    return new PipeTransferSchemaSnapshotSealReq()
        .convertToTPipeTransferSnapshotSealBytes(fileName, fileLength);
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    return obj instanceof PipeTransferSchemaSnapshotSealReq && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
