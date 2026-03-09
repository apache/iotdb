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
import org.apache.iotdb.db.pipe.processor.twostage.state.State;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CombineRequest extends TPipeTransferReq {

  private String pipeName;
  private long creationTime;
  private int regionId;
  private String combineId;

  private State state;

  private CombineRequest() {
    // Empty constructor
  }

  public String getPipeName() {
    return pipeName;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public int getRegionId() {
    return regionId;
  }

  public String getCombineId() {
    return combineId;
  }

  public State getState() {
    return state;
  }

  public static CombineRequest toTPipeTransferReq(
      String pipeName, long creationTime, int regionId, String combineId, State state)
      throws IOException {
    return new CombineRequest()
        .convertToTPipeTransferReq(pipeName, creationTime, regionId, combineId, state);
  }

  public static CombineRequest fromTPipeTransferReq(TPipeTransferReq transferReq) throws Exception {
    return new CombineRequest().translateFromTPipeTransferReq(transferReq);
  }

  private CombineRequest convertToTPipeTransferReq(
      String pipeName, long creationTime, int regionId, String combineId, State state)
      throws IOException {
    this.pipeName = pipeName;
    this.creationTime = creationTime;
    this.regionId = regionId;
    this.state = state;
    this.combineId = combineId;

    this.version = IoTDBSinkRequestVersion.VERSION_2.getVersion();
    this.type = RequestType.COMBINE.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(pipeName, outputStream);
      ReadWriteIOUtils.write(creationTime, outputStream);
      ReadWriteIOUtils.write(regionId, outputStream);
      ReadWriteIOUtils.write(combineId, outputStream);

      ReadWriteIOUtils.write(state.getClass().getName(), outputStream);
      state.serialize(outputStream);

      this.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return this;
  }

  private CombineRequest translateFromTPipeTransferReq(TPipeTransferReq transferReq)
      throws Exception {
    pipeName = ReadWriteIOUtils.readString(transferReq.body);
    creationTime = ReadWriteIOUtils.readLong(transferReq.body);
    regionId = ReadWriteIOUtils.readInt(transferReq.body);
    combineId = ReadWriteIOUtils.readString(transferReq.body);

    final String stateClassName = ReadWriteIOUtils.readString(transferReq.body);
    state = (State) Class.forName(stateClassName).newInstance();
    state.deserialize(transferReq.body);

    version = transferReq.version;
    type = transferReq.type;

    return this;
  }

  @Override
  public String toString() {
    return "CombineRequest{"
        + "pipeName='"
        + pipeName
        + '\''
        + ", creationTime="
        + creationTime
        + ", regionId="
        + regionId
        + ", combineId='"
        + combineId
        + '\''
        + ", state="
        + state
        + '}';
  }
}
