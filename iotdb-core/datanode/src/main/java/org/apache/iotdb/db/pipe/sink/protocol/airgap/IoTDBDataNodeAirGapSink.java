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

package org.apache.iotdb.db.pipe.sink.protocol.airgap;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.commons.pipe.sink.protocol.IoTDBAirGapSink;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferDataNodeHandshakeV1Req;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferDataNodeHandshakeV2Req;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;

import java.io.IOException;
import java.util.HashMap;

@TreeModel
@TableModel
public abstract class IoTDBDataNodeAirGapSink extends IoTDBAirGapSink {

  @Override
  protected boolean mayNeedHandshakeWhenFail() {
    return false;
  }

  @Override
  protected byte[] generateHandShakeV1Payload() throws IOException {
    return PipeTransferDataNodeHandshakeV1Req.toTPipeTransferBytes(
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
  }

  @Override
  protected byte[] generateHandShakeV2Payload() throws IOException {
    final HashMap<String, String> params = new HashMap<>();
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLUSTER_ID,
        IoTDBDescriptor.getInstance().getConfig().getClusterId());
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_TIME_PRECISION,
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_CONVERT_ON_TYPE_MISMATCH,
        Boolean.toString(shouldReceiverConvertOnTypeMismatch));
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_LOAD_TSFILE_STRATEGY, loadTsFileStrategy);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USER_ID, userId);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USERNAME, username);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLI_HOSTNAME, cliHostname);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PASSWORD, password);
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_VALIDATE_TSFILE,
        Boolean.toString(loadTsFileValidation));
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_MARK_AS_PIPE_REQUEST,
        Boolean.toString(shouldMarkAsPipeRequest));
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_SKIP_IF, Boolean.toString(skipIfNoPrivileges));

    return PipeTransferDataNodeHandshakeV2Req.toTPipeTransferBytes(params);
  }
}
