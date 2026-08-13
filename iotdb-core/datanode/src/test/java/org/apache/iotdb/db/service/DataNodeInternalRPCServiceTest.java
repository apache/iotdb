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

package org.apache.iotdb.db.service;

import org.junit.Assert;
import org.junit.Test;

public class DataNodeInternalRPCServiceTest {

  @Test
  public void testServiceReadsLatestDataNodeReadinessContext() {
    DataNodeInternalRPCService service = DataNodeInternalRPCService.getInstance();
    try {
      DataNode.ConsensusReadinessContext oldContext = new DataNode.ConsensusReadinessContext();
      service.setConsensusReadiness(oldContext);
      ConsensusReadiness serviceContext = service.getConsensusReadiness();

      DataNode.ConsensusReadinessContext newContext = new DataNode.ConsensusReadinessContext();
      service.setConsensusReadiness(newContext);
      oldContext.markSchemaRegionConsensusStarted();
      oldContext.markDataRegionConsensusStarted();
      Assert.assertFalse(serviceContext.isAllConsensusStarted());

      newContext.markSchemaRegionConsensusStarted();
      newContext.markDataRegionConsensusStarted();
      Assert.assertTrue(serviceContext.isAllConsensusStarted());
    } finally {
      service.setConsensusReadiness(new DataNode.ConsensusReadinessContext());
    }
  }
}
