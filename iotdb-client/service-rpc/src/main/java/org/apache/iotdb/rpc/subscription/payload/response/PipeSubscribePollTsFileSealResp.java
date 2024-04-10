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

package org.apache.iotdb.rpc.subscription.payload.response;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Objects;

public class PipeSubscribePollTsFileSealResp extends TPipeSubscribeResp {

  private transient String fileName;
  private transient long fileLength;
  private transient String subscriptionCommitId;

  public final String getFileName() {
    return fileName;
  }

  public final long getFileLength() {
    return fileLength;
  }

  public String getSubscriptionCommitId() {
    return subscriptionCommitId;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeSubscribePollTsFileSealResp toTPipeSubscribeResp(
      TSStatus status, String fileName, long fileLength, String subscriptionCommitId) {
    final PipeSubscribePollTsFileSealResp resp = new PipeSubscribePollTsFileSealResp();

    resp.fileName = fileName;
    resp.fileLength = fileLength;
    resp.subscriptionCommitId = subscriptionCommitId;

    resp.status = status;
    resp.version = PipeSubscribeResponseVersion.VERSION_1.getVersion();
    resp.type = PipeSubscribeResponseType.POLL_TS_FILE_SEAL.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(fileName, outputStream);
      ReadWriteIOUtils.write(fileLength, outputStream);
      ReadWriteIOUtils.write(subscriptionCommitId, outputStream);
      resp.body =
          Collections.singletonList(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
    } catch (final IOException e) {
      resp.status =
          RpcUtils.getStatus(TSStatusCode.SUBSCRIPTION_POLL_TS_FILE_ERROR, e.getMessage());
    }

    return resp;
  }

  public static PipeSubscribePollTsFileSealResp fromTPipeSubscribeResp(
      TPipeSubscribeResp pollTsFileSealResp) {
    final PipeSubscribePollTsFileSealResp resp = new PipeSubscribePollTsFileSealResp();

    if (Objects.nonNull(pollTsFileSealResp.body)) {
      for (final ByteBuffer byteBuffer : pollTsFileSealResp.body) {
        if (Objects.nonNull(byteBuffer) && byteBuffer.hasRemaining()) {
          resp.fileName = ReadWriteIOUtils.readString(byteBuffer);
          resp.fileLength = ReadWriteIOUtils.readLong(byteBuffer);
          resp.subscriptionCommitId = ReadWriteIOUtils.readString(byteBuffer);
        }
      }
    }

    resp.status = pollTsFileSealResp.status;
    resp.version = pollTsFileSealResp.version;
    resp.type = pollTsFileSealResp.type;
    resp.body = pollTsFileSealResp.body;

    return resp;
  }

  /////////////////////////////// Object ///////////////////////////////

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PipeSubscribePollTsFileSealResp that = (PipeSubscribePollTsFileSealResp) obj;
    return Objects.equals(this.fileName, that.fileName)
        && fileLength == that.fileLength
        && Objects.equals(this.subscriptionCommitId, that.subscriptionCommitId)
        && Objects.equals(this.status, that.status)
        && this.version == that.version
        && this.type == that.type
        && Objects.equals(this.body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileName, fileLength, subscriptionCommitId, status, version, type, body);
  }
}
