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

package org.apache.iotdb.db.pipe.connector.payload.evolvable.builder;

import org.apache.iotdb.db.pipe.connector.protocol.thrift.sync.IoTDBThriftSyncConnector;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

public class IoTDBThriftSyncPipeTransferBatchReqBuilder extends PipeTransferBatchReqBuilder {

  public IoTDBThriftSyncPipeTransferBatchReqBuilder(PipeParameters parameters) {
    super(parameters);
  }

  public void onSuccess(IoTDBThriftSyncConnector connector) {
    reqs.clear();

    for (int i = 0; i < events.size(); ++i) {
      connector.commit(
          requestCommitIds.get(i),
          events.get(i) instanceof EnrichedEvent ? (EnrichedEvent) events.get(i) : null);
    }
    events.clear();
    requestCommitIds.clear();

    firstEventProcessingTime = Long.MIN_VALUE;

    bufferSize = 0;
  }
}
