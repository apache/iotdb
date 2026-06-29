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

package org.apache.iotdb.confignode.manager.pipe.agent.receiver;

import org.apache.iotdb.commons.pipe.receiver.IoTDBReceiver;
import org.apache.iotdb.commons.pipe.receiver.IoTDBReceiverAgent;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.pipe.receiver.protocol.IoTDBConfigNodeReceiver;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class IoTDBConfigNodeReceiverAgent extends IoTDBReceiverAgent {

  private final ConcurrentMap<String, IoTDBReceiver> clientKey2ReceiverMap =
      new ConcurrentHashMap<>();

  @Override
  protected void initConstructors() {
    RECEIVER_CONSTRUCTORS.put(
        IoTDBSinkRequestVersion.VERSION_1.getVersion(), IoTDBConfigNodeReceiver::new);
  }

  @Override
  protected IoTDBReceiver getReceiverWithSpecifiedClient(final String key) {
    return clientKey2ReceiverMap.get(key);
  }

  @Override
  protected void setReceiverWithSpecifiedClient(final String key, final IoTDBReceiver receiver) {
    clientKey2ReceiverMap.put(key, receiver);
  }

  @Override
  protected void removeReceiverWithSpecifiedClient(final String key) {
    clientKey2ReceiverMap.remove(key);
  }

  public void cleanPipeReceiverDir() {
    cleanPipeReceiverDir(
        new File(ConfigNodeDescriptor.getInstance().getConf().getPipeReceiverFileDir()));
  }
}
