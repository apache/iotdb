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

package org.apache.iotdb.session.subscription.payload.response;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PipeSubscribeHandshakeResp extends TPipeSubscribeResp {
  private transient List<TEndPoint> endPoints = new ArrayList<>();

  /////////////////////////////// Thrift ///////////////////////////////

  /**
   * Serialize the incoming parameters into `PipeSubscribeHandshakeResp`, called by the subscription
   * server.
   */
  public static PipeSubscribeHandshakeResp toTPipeSubscribeResp(List<TEndPoint> endPoints)
      throws IOException {
    final PipeSubscribeHandshakeResp resp = new PipeSubscribeHandshakeResp();

    resp.endPoints = endPoints;

    resp.version = PipeSubscribeResponseVersion.VERSION_1.getVersion();
    resp.type = PipeSubscribeResponseType.ACK.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(endPoints.size(), outputStream);
      for (TEndPoint endPoint : endPoints) {
        ReadWriteIOUtils.write(endPoint.ip, outputStream);
        ReadWriteIOUtils.write(endPoint.port, outputStream);
      }
      resp.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return resp;
  }

  /**
   * Deserialize `PipeSubscribeHandshakeResp` to obtain parameters, called by the subscription
   * client.
   */
  public static PipeSubscribeHandshakeResp fromTPipeSubscribeResp(
      PipeSubscribeHandshakeResp handshakeResp) {
    final PipeSubscribeHandshakeResp resp = new PipeSubscribeHandshakeResp();

    int size = ReadWriteIOUtils.readInt(handshakeResp.body);
    for (int i = 0; i < size; ++i) {
      final String ip = ReadWriteIOUtils.readString(handshakeResp.body);
      final int port = ReadWriteIOUtils.readInt(handshakeResp.body);
      resp.endPoints.add(new TEndPoint(ip, port));
    }

    resp.version = handshakeResp.version;
    resp.type = handshakeResp.type;
    resp.body = handshakeResp.body;

    return resp;
  }
}
