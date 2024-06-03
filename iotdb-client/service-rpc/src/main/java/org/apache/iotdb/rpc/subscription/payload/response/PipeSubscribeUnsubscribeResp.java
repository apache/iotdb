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
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;

import java.util.Objects;

public class PipeSubscribeUnsubscribeResp extends TPipeSubscribeResp {

  /////////////////////////////// Thrift ///////////////////////////////

  /**
   * Serialize the incoming parameters into `PipeSubscribeUnsubscribeResp`, called by the
   * subscription server.
   */
  public static PipeSubscribeUnsubscribeResp toTPipeSubscribeResp(final TSStatus status) {
    final PipeSubscribeUnsubscribeResp resp = new PipeSubscribeUnsubscribeResp();

    resp.status = status;
    resp.version = PipeSubscribeResponseVersion.VERSION_1.getVersion();
    resp.type = PipeSubscribeResponseType.ACK.getType();

    return resp;
  }

  /** Deserialize `TPipeSubscribeResp` to obtain parameters, called by the subscription client. */
  public static PipeSubscribeUnsubscribeResp fromTPipeSubscribeResp(
      final TPipeSubscribeResp unsubscribeResp) {
    final PipeSubscribeUnsubscribeResp resp = new PipeSubscribeUnsubscribeResp();

    resp.status = unsubscribeResp.status;
    resp.version = unsubscribeResp.version;
    resp.type = unsubscribeResp.type;
    resp.body = unsubscribeResp.body;

    return resp;
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PipeSubscribeUnsubscribeResp that = (PipeSubscribeUnsubscribeResp) obj;
    return Objects.equals(this.status, that.status)
        && this.version == that.version
        && this.type == that.type
        && Objects.equals(this.body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, version, type, body);
  }
}
