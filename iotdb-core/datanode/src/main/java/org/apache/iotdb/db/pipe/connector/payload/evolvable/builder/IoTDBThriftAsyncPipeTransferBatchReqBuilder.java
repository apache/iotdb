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

import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

import java.util.ArrayList;
import java.util.List;

public class IoTDBThriftAsyncPipeTransferBatchReqBuilder extends PipeTransferBatchReqBuilder {

  public IoTDBThriftAsyncPipeTransferBatchReqBuilder(PipeParameters parameters) {
    super(parameters);
  }

  public List<Event> deepcopyEvents() {
    return new ArrayList<>(events);
  }

  public List<Long> deepcopyRequestCommitIds() {
    return new ArrayList<>(requestCommitIds);
  }

  public long getLastCommitId() {
    return requestCommitIds.isEmpty() ? -1 : requestCommitIds.get(requestCommitIds.size() - 1);
  }
}
