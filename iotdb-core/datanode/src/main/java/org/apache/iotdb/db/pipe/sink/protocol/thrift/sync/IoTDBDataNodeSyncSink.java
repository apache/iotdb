/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.sink.protocol.thrift.sync;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClientManager;
import org.apache.iotdb.commons.pipe.sink.protocol.IoTDBSslSyncSink;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.pipe.sink.client.IoTDBDataNodeSyncClientManager;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;

import java.util.List;
import java.util.Objects;

@TreeModel
@TableModel
public abstract class IoTDBDataNodeSyncSink extends IoTDBSslSyncSink {

  protected IoTDBDataNodeSyncClientManager clientManager;

  @Override
  protected IoTDBSyncClientManager constructClient(
      final List<TEndPoint> nodeUrls,
      final boolean useSSL,
      final String trustStorePath,
      final String trustStorePwd,
      /* The following parameters are used locally. */
      final boolean useLeaderCache,
      final String loadBalanceStrategy,
      /* The following parameters are used to handshake with the receiver. */
      final UserEntity userEntity,
      final String password,
      final boolean shouldReceiverConvertOnTypeMismatch,
      final String loadTsFileStrategy,
      final boolean validateTsFile,
      final boolean shouldMarkAsPipeRequest,
      final boolean skipIfNoPrivileges) {
    clientManager =
        new IoTDBDataNodeSyncClientManager(
            nodeUrls,
            useSSL,
            Objects.nonNull(trustStorePath) ? IoTDBConfig.addDataHomeDir(trustStorePath) : null,
            trustStorePwd,
            useLeaderCache,
            loadBalanceStrategy,
            userEntity,
            password,
            shouldReceiverConvertOnTypeMismatch,
            loadTsFileStrategy,
            validateTsFile,
            shouldMarkAsPipeRequest,
            skipIfNoPrivileges);
    return clientManager;
  }
}
