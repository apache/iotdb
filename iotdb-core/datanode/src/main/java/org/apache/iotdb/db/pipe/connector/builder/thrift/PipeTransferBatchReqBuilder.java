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

package org.apache.iotdb.db.pipe.connector.builder.thrift;

import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_BATCH_DELAY_DEFAULT_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_BATCH_DELAY_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_BATCH_SIZE_DEFAULT_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_BATCH_SIZE_KEY;

public abstract class PipeTransferBatchReqBuilder {

  protected final List<TPipeTransferReq> reqs = new ArrayList<>();
  protected final List<Event> events = new ArrayList<>();

  // limit in delayed time
  protected final int maxDelayInMs;
  protected long firstEventProcessingTime = Long.MIN_VALUE;

  // limit in buffer size
  protected final long maxBatchSizeInBytes;
  protected long bufferSize = 0;

  protected PipeTransferBatchReqBuilder(PipeParameters parameters) {
    maxDelayInMs =
        parameters.getIntOrDefault(
                CONNECTOR_IOTDB_BATCH_DELAY_KEY, CONNECTOR_IOTDB_BATCH_DELAY_DEFAULT_VALUE)
            * 1000;
    maxBatchSizeInBytes =
        parameters.getLongOrDefault(
            CONNECTOR_IOTDB_BATCH_SIZE_KEY, CONNECTOR_IOTDB_BATCH_SIZE_DEFAULT_VALUE);
  }

  public List<TPipeTransferReq> getTPipeTransferReqs() {
    return reqs;
  }

  public boolean isEmpty() {
    return reqs.isEmpty();
  }
}
