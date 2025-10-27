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

package org.apache.iotdb.db.pipe.processor.twostage.exchange.payload;

import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class FetchCombineResultRequest extends TPipeTransferReq {

  private String pipeName;
  private long creationTime;
  private List<String> combineIdList;

  private FetchCombineResultRequest() {
    // Empty constructor
  }

  public String getPipeName() {
    return pipeName;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public List<String> getCombineIdList() {
    return combineIdList;
  }

  public static FetchCombineResultRequest toTPipeTransferReq(
      String pipeName, long creationTime, List<String> combineIdList) throws IOException {
    return new FetchCombineResultRequest()
        .convertToTPipeTransferReq(pipeName, creationTime, combineIdList);
  }

  public static FetchCombineResultRequest fromTPipeTransferReq(TPipeTransferReq transferReq)
      throws Exception {
    return new FetchCombineResultRequest().translateFromTPipeTransferReq(transferReq);
  }

  private FetchCombineResultRequest convertToTPipeTransferReq(
      String pipeName, long creationTime, List<String> combineIdList) throws IOException {
    this.pipeName = pipeName;
    this.creationTime = creationTime;
    this.combineIdList = combineIdList;

    this.version = IoTDBSinkRequestVersion.VERSION_2.getVersion();
    this.type = RequestType.FETCH_COMBINE_RESULT.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(pipeName, outputStream);
      ReadWriteIOUtils.write(creationTime, outputStream);

      ReadWriteIOUtils.write(combineIdList.size(), outputStream);
      for (String combineId : combineIdList) {
        ReadWriteIOUtils.write(combineId, outputStream);
      }

      this.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return this;
  }

  private FetchCombineResultRequest translateFromTPipeTransferReq(TPipeTransferReq transferReq) {
    pipeName = ReadWriteIOUtils.readString(transferReq.body);
    creationTime = ReadWriteIOUtils.readLong(transferReq.body);
    combineIdList = new ArrayList<>();
    final int combineIdListSize = ReadWriteIOUtils.readInt(transferReq.body);
    for (int i = 0; i < combineIdListSize; i++) {
      combineIdList.add(ReadWriteIOUtils.readString(transferReq.body));
    }

    version = transferReq.version;
    type = transferReq.type;

    return this;
  }

  @Override
  public String toString() {
    return "FetchCombineResultRequest{"
        + "pipeName='"
        + pipeName
        + '\''
        + ", creationTime="
        + creationTime
        + ", combineIdList="
        + combineIdList
        + '}';
  }
}
