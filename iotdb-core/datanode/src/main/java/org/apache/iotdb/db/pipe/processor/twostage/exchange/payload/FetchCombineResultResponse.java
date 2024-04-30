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

import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class FetchCombineResultResponse extends TPipeTransferResp {

  public enum CombineResultType {
    SUCCESS,
    INCOMPLETE,
    OUTDATED,
  }

  private Map<String, CombineResultType> combineId2ResultType = new HashMap<>();

  private FetchCombineResultResponse() {
    // Empty constructor
  }

  public Map<String, CombineResultType> getCombineId2ResultType() {
    return combineId2ResultType;
  }

  public static FetchCombineResultResponse toTPipeTransferResp(
      Map<String, CombineResultType> combineId2ResultType) throws IOException {
    final FetchCombineResultResponse response = new FetchCombineResultResponse();

    response.combineId2ResultType = combineId2ResultType;

    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(combineId2ResultType.size(), outputStream);
      for (Map.Entry<String, CombineResultType> entry : combineId2ResultType.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), outputStream);
        ReadWriteIOUtils.write(entry.getValue().ordinal(), outputStream);
      }

      response.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    response.status = RpcUtils.SUCCESS_STATUS;

    return response;
  }

  public static FetchCombineResultResponse fromTPipeTransferResp(TPipeTransferResp transferResp) {
    final FetchCombineResultResponse response = new FetchCombineResultResponse();

    response.status = transferResp.status;
    response.body = transferResp.body;

    response.combineId2ResultType = new HashMap<>();
    if (response.isSetBody()) {
      final int size = ReadWriteIOUtils.readInt(transferResp.body);
      for (int i = 0; i < size; i++) {
        final String combineId = ReadWriteIOUtils.readString(transferResp.body);
        final CombineResultType resultType =
            CombineResultType.values()[ReadWriteIOUtils.readInt(transferResp.body)];
        response.combineId2ResultType.put(combineId, resultType);
      }
    }

    return response;
  }

  @Override
  public String toString() {
    return "FetchCombineResultResponse{" + "combineId2ResultType=" + combineId2ResultType + '}';
  }
}
