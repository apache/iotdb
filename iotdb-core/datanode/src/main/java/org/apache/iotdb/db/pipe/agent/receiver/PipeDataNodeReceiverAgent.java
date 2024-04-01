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

package org.apache.iotdb.db.pipe.agent.receiver;

import org.apache.iotdb.commons.pipe.receiver.IoTDBReceiverAgent;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.receiver.protocol.airgap.IoTDBAirGapReceiverAgent;
import org.apache.iotdb.db.pipe.receiver.protocol.legacy.IoTDBLegacyPipeReceiverAgent;
import org.apache.iotdb.db.pipe.receiver.protocol.thrift.IoTDBDataNodeReceiverAgent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;

/** {@link PipeDataNodeReceiverAgent} is the entry point of all pipe receivers' logic. */
public class PipeDataNodeReceiverAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeDataNodeReceiverAgent.class);

  private final IoTDBDataNodeReceiverAgent thriftAgent;
  private final IoTDBAirGapReceiverAgent airGapAgent;
  private final IoTDBLegacyPipeReceiverAgent legacyAgent;

  public PipeDataNodeReceiverAgent() {
    thriftAgent = new IoTDBDataNodeReceiverAgent();
    airGapAgent = new IoTDBAirGapReceiverAgent();
    legacyAgent = new IoTDBLegacyPipeReceiverAgent();
  }

  public IoTDBDataNodeReceiverAgent thrift() {
    return thriftAgent;
  }

  public IoTDBAirGapReceiverAgent airGap() {
    return airGapAgent;
  }

  public IoTDBLegacyPipeReceiverAgent legacy() {
    return legacyAgent;
  }

  public void cleanPipeReceiverDirs() {
    String[] pipeReceiverFileDirs =
        IoTDBDescriptor.getInstance().getConfig().getPipeReceiverFileDirs();
    Arrays.stream(pipeReceiverFileDirs)
        .map(File::new)
        .forEach(IoTDBReceiverAgent::cleanPipeReceiverDir);
  }
}
