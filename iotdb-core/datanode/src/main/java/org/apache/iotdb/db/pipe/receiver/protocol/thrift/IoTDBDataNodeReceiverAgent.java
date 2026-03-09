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

package org.apache.iotdb.db.pipe.receiver.protocol.thrift;

import org.apache.iotdb.commons.pipe.receiver.IoTDBReceiver;
import org.apache.iotdb.commons.pipe.receiver.IoTDBReceiverAgent;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.db.pipe.processor.twostage.exchange.receiver.TwoStageAggregateReceiver;

public class IoTDBDataNodeReceiverAgent extends IoTDBReceiverAgent {

  private final ThreadLocal<IoTDBReceiver> receiverThreadLocal = new ThreadLocal<>();

  @Override
  protected void initConstructors() {
    RECEIVER_CONSTRUCTORS.put(
        IoTDBSinkRequestVersion.VERSION_1.getVersion(), IoTDBDataNodeReceiver::new);
    RECEIVER_CONSTRUCTORS.put(
        IoTDBSinkRequestVersion.VERSION_2.getVersion(), TwoStageAggregateReceiver::new);
  }

  @Override
  protected IoTDBReceiver getReceiverWithSpecifiedClient(final String ignore) {
    return receiverThreadLocal.get();
  }

  @Override
  protected void setReceiverWithSpecifiedClient(final String ignore, final IoTDBReceiver receiver) {
    receiverThreadLocal.set(receiver);
  }

  @Override
  protected void removeReceiverWithSpecifiedClient(final String ignore) {
    receiverThreadLocal.remove();
  }
}
