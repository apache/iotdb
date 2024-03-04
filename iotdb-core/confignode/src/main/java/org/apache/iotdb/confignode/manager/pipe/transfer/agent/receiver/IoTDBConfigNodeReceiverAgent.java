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

package org.apache.iotdb.confignode.manager.pipe.transfer.agent.receiver;

import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.IoTDBConnectorRequestVersion;
import org.apache.iotdb.commons.pipe.receiver.IoTDBReceiverAgent;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class IoTDBConfigNodeReceiverAgent extends IoTDBReceiverAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConfigNodeReceiverAgent.class);

  @Override
  protected void initConstructors() {
    RECEIVER_CONSTRUCTORS.put(
        IoTDBConnectorRequestVersion.VERSION_1.getVersion(), IoTDBConfigNodeReceiver::new);
  }

  public void cleanPipeReceiverDir() {
    cleanPipeReceiverDir(
        new File(ConfigNodeDescriptor.getInstance().getConf().getPipeReceiverFileDir()));
  }

  private static void cleanPipeReceiverDir(File receiverFileDir) {
    try {
      FileUtils.deleteDirectory(receiverFileDir);
      LOGGER.info("Clean pipe receiver dir {} successfully.", receiverFileDir);
    } catch (Exception e) {
      LOGGER.warn("Clean pipe receiver dir {} failed.", receiverFileDir, e);
    }

    try {
      FileUtils.forceMkdir(receiverFileDir);
      LOGGER.info("Create pipe receiver dir {} successfully.", receiverFileDir);
    } catch (IOException e) {
      LOGGER.warn("Create pipe receiver dir {} failed.", receiverFileDir, e);
    }
  }
}
