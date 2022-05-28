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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.statemachine.SchemaRegionStateMachine;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;

import java.io.File;

/**
 * We can use SchemaRegionConsensusImpl.getInstance() to obtain a consensus layer reference for
 * schemaRegion's reading and writing
 */
public class SchemaRegionConsensusImpl {

  private SchemaRegionConsensusImpl() {}

  public static IConsensus getInstance() {
    return SchemaRegionConsensusImplHolder.INSTANCE;
  }

  private static class SchemaRegionConsensusImplHolder {

    private static final IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();
    private static final IConsensus INSTANCE =
        ConsensusFactory.getConsensusImpl(
                conf.getSchemaRegionConsensusProtocolClass(),
                new TEndPoint(conf.getInternalIp(), conf.getSchemaRegionConsensusPort()),
                new File(conf.getSchemaRegionConsensusDir()),
                gid ->
                    new SchemaRegionStateMachine(
                        SchemaEngine.getInstance().getSchemaRegion((SchemaRegionId) gid)))
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format(
                            ConsensusFactory.CONSTRUCT_FAILED_MSG,
                            conf.getSchemaRegionConsensusProtocolClass())));

    private SchemaRegionConsensusImplHolder() {}
  }
}
