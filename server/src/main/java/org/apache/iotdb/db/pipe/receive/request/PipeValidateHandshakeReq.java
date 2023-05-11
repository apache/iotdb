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

package org.apache.iotdb.db.pipe.receive.request;

import org.apache.iotdb.service.rpc.thrift.TPipeHandshakeReq;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class PipeValidateHandshakeReq extends TPipeHandshakeReq {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeValidateHandshakeReq.class);
  private final String timestampPrecision;

  public PipeValidateHandshakeReq(
      String pipeVersion, String iotdbVersion, String timestampPrecision) {
    this.pipeVersion = pipeVersion;
    this.iotdbVersion = iotdbVersion;
    this.timestampPrecision = timestampPrecision;
  }

  public String getTimestampPrecision() {
    return timestampPrecision;
  }

  public TPipeHandshakeReq toTPipeHandshakeReq() throws IOException {
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(timestampPrecision, outputStream);
      this.handshakeInfo =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      return this;
    }
  }

  public static PipeValidateHandshakeReq fromTPipeHandshakeReq(TPipeHandshakeReq req) {
    return new PipeValidateHandshakeReq(
        req.getPipeVersion(),
        req.getIotdbVersion(),
        ReadWriteIOUtils.readString(req.handshakeInfo));
  }
}
