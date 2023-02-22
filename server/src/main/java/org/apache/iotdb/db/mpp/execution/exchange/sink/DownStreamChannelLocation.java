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

package org.apache.iotdb.db.mpp.execution.exchange.sink;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DownStreamChannelLocation {

  // We fill these fields util FI was produced
  private TEndPoint remoteEndpoint;
  private TFragmentInstanceId remoteFragmentInstanceId;

  private final String remotePlanNodeId;

  /**
   * @param remoteEndpoint Hostname and Port of the remote fragment instance where the data blocks
   *     should be sent to.
   * @param remoteFragmentInstanceId The ID of the remote fragment instance.
   * @param remotePlanNodeId The plan node ID of the remote exchangeNode.
   */
  public DownStreamChannelLocation(
      TEndPoint remoteEndpoint,
      TFragmentInstanceId remoteFragmentInstanceId,
      String remotePlanNodeId) {
    this.remoteEndpoint = remoteEndpoint;
    this.remoteFragmentInstanceId = remoteFragmentInstanceId;
    this.remotePlanNodeId = remotePlanNodeId;
  }

  public DownStreamChannelLocation(String remotePlanNodeId) {
    this.remoteEndpoint = null;
    this.remoteFragmentInstanceId = null;
    this.remotePlanNodeId = remotePlanNodeId;
  }

  public void setRemoteEndpoint(TEndPoint remoteEndpoint) {
    this.remoteEndpoint = remoteEndpoint;
  }

  public void setRemoteFragmentInstanceId(TFragmentInstanceId remoteFragmentInstanceId) {
    this.remoteFragmentInstanceId = remoteFragmentInstanceId;
  }

  public TEndPoint getRemoteEndpoint() {
    return remoteEndpoint;
  }

  public TFragmentInstanceId getRemoteFragmentInstanceId() {
    return remoteFragmentInstanceId;
  }

  public String getRemotePlanNodeId() {
    return remotePlanNodeId;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(remoteEndpoint.getIp(), byteBuffer);
    ReadWriteIOUtils.write(remoteEndpoint.getPort(), byteBuffer);
    ReadWriteIOUtils.write(remoteFragmentInstanceId.getQueryId(), byteBuffer);
    ReadWriteIOUtils.write(remoteFragmentInstanceId.getFragmentId(), byteBuffer);
    ReadWriteIOUtils.write(remoteFragmentInstanceId.getInstanceId(), byteBuffer);

    ReadWriteIOUtils.write(remotePlanNodeId, byteBuffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(remoteEndpoint.getIp(), stream);
    ReadWriteIOUtils.write(remoteEndpoint.getPort(), stream);
    ReadWriteIOUtils.write(remoteFragmentInstanceId.getQueryId(), stream);
    ReadWriteIOUtils.write(remoteFragmentInstanceId.getFragmentId(), stream);
    ReadWriteIOUtils.write(remoteFragmentInstanceId.getInstanceId(), stream);

    ReadWriteIOUtils.write(remotePlanNodeId, stream);
  }

  public static DownStreamChannelLocation deserialize(ByteBuffer byteBuffer) {
    TEndPoint endPoint =
        new TEndPoint(
            ReadWriteIOUtils.readString(byteBuffer), ReadWriteIOUtils.readInt(byteBuffer));
    TFragmentInstanceId fragmentInstanceId =
        new TFragmentInstanceId(
            ReadWriteIOUtils.readString(byteBuffer),
            ReadWriteIOUtils.readInt(byteBuffer),
            ReadWriteIOUtils.readString(byteBuffer));
    String remotePlanNodeId = ReadWriteIOUtils.readString(byteBuffer);
    return new DownStreamChannelLocation(endPoint, fragmentInstanceId, remotePlanNodeId);
  }
}
