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

package org.apache.iotdb.db.consensus;

import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.statemachine.DataRegionStateMachine;
import org.apache.iotdb.db.consensus.statemachine.SchemaRegionStateMachine;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;

import java.io.File;

/**
 * We can use ConsensusImpl.getInstance() to obtain a consensus layer reference for reading and
 * writing
 */
public class ConsensusImpl {

  private ConsensusImpl() {}

  public static IConsensus getInstance() {
    return ConsensusImplHolder.INSTANCE;
  }

  private static class ConsensusImplHolder {

    private static final IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();
    private static final IConsensus INSTANCE =
        ConsensusFactory.getConsensusImpl(
                conf.getConsensusProtocolClass(),
                new Endpoint(conf.getInternalIp(), conf.getConsensusPort()),
                new File(conf.getConsensusDir()),
                gid -> {
                  switch (gid.getType()) {
                    case SchemaRegion:
                      return new SchemaRegionStateMachine(
                          SchemaEngine.getInstance().getSchemaRegion((SchemaRegionId) gid));
                    case DataRegion:
                      return new DataRegionStateMachine(
                          StorageEngineV2.getInstance().getDataRegion((DataRegionId) gid));
                  }
                  throw new IllegalArgumentException(
                      String.format("Unexpected consensusGroup %s", gid));
                })
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format(
                            ConsensusFactory.CONSTRUCT_FAILED_MSG,
                            conf.getConsensusProtocolClass())));

    private ConsensusImplHolder() {}
  }
}
