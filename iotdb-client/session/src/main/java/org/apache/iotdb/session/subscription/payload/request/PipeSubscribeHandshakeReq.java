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

package org.apache.iotdb.session.subscription.payload.request;

import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class PipeSubscribeHandshakeReq extends TPipeSubscribeReq {

  private transient String consumerClientID;
  private transient String consumerGroupID;

  /////////////////////////////// Thrift ///////////////////////////////

  /**
   * Serialize the incoming parameters into `PipeSubscribeHandshakeReq`, called by the subscription
   * client.
   */
  public static PipeSubscribeHandshakeReq toTPipeSubscribeReq(
      String consumerClientID, String consumerGroupID) throws IOException {
    final PipeSubscribeHandshakeReq req = new PipeSubscribeHandshakeReq();

    req.consumerClientID = consumerClientID;
    req.consumerGroupID = consumerGroupID;

    req.version = PipeSubscribeRequestVersion.VERSION_1.getVersion();
    req.type = PipeSubscribeRequestType.HANDSHAKE.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(consumerClientID, outputStream);
      ReadWriteIOUtils.write(consumerGroupID, outputStream);
      req.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return req;
  }

  /**
   * Deserialize `PipeSubscribeHandshakeReq` to obtain parameters, called by the subscription
   * server.
   */
  public static PipeSubscribeHandshakeReq fromTPipeSubscribeReq(
      PipeSubscribeHandshakeReq handshakeReq) {
    final PipeSubscribeHandshakeReq req = new PipeSubscribeHandshakeReq();

    req.consumerClientID = ReadWriteIOUtils.readString(handshakeReq.body);
    req.consumerGroupID = ReadWriteIOUtils.readString(handshakeReq.body);

    req.version = handshakeReq.version;
    req.type = handshakeReq.type;
    req.body = handshakeReq.body;

    return req;
  }
}
