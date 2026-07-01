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

package org.apache.iotdb.commons.pipe.agent.task.meta;

import org.junit.Assert;
import org.junit.Test;

public class PipeTemporaryMetaTest {

  @Test
  public void testTsFileEpochDegradedStatusCodec() {
    Assert.assertEquals(
        PipeTemporaryMeta.TS_FILE_EPOCH_DEGRADED_STATUS_UNKNOWN,
        PipeTemporaryMeta.encodeTsFileEpochDegradedStatus(null));
    Assert.assertEquals(
        PipeTemporaryMeta.TS_FILE_EPOCH_DEGRADED_STATUS_FALSE,
        PipeTemporaryMeta.encodeTsFileEpochDegradedStatus(false));
    Assert.assertEquals(
        PipeTemporaryMeta.TS_FILE_EPOCH_DEGRADED_STATUS_TRUE,
        PipeTemporaryMeta.encodeTsFileEpochDegradedStatus(true));

    Assert.assertNull(PipeTemporaryMeta.decodeTsFileEpochDegradedStatus(null));
    Assert.assertNull(
        PipeTemporaryMeta.decodeTsFileEpochDegradedStatus(
            PipeTemporaryMeta.TS_FILE_EPOCH_DEGRADED_STATUS_UNKNOWN));
    Assert.assertEquals(
        Boolean.FALSE,
        PipeTemporaryMeta.decodeTsFileEpochDegradedStatus(
            PipeTemporaryMeta.TS_FILE_EPOCH_DEGRADED_STATUS_FALSE));
    Assert.assertEquals(
        Boolean.TRUE,
        PipeTemporaryMeta.decodeTsFileEpochDegradedStatus(
            PipeTemporaryMeta.TS_FILE_EPOCH_DEGRADED_STATUS_TRUE));
  }

  @Test
  public void testAgentAggregatesTsFileEpochDegradedStatus() {
    final PipeTemporaryMetaInAgent temporaryMeta = new PipeTemporaryMetaInAgent("test_pipe", 1L);

    Assert.assertNull(temporaryMeta.getGlobalTsFileEpochDegraded());

    temporaryMeta.setTsFileEpochDegraded(1, false);
    temporaryMeta.setTsFileEpochDegraded(2, false);
    Assert.assertEquals(Boolean.FALSE, temporaryMeta.getGlobalTsFileEpochDegraded());

    temporaryMeta.setTsFileEpochDegraded(2, true);
    Assert.assertEquals(Boolean.TRUE, temporaryMeta.getGlobalTsFileEpochDegraded());

    temporaryMeta.clearTsFileEpochDegraded(2);
    Assert.assertEquals(Boolean.FALSE, temporaryMeta.getGlobalTsFileEpochDegraded());

    temporaryMeta.clearTsFileEpochDegraded(1);
    Assert.assertNull(temporaryMeta.getGlobalTsFileEpochDegraded());
  }

  @Test
  public void testCoordinatorAggregatesNullableDegradedStatus() {
    final PipeTemporaryMetaInCoordinator temporaryMeta = new PipeTemporaryMetaInCoordinator();

    Assert.assertNull(temporaryMeta.getGlobalDegraded());

    temporaryMeta.setDegraded(1, false);
    temporaryMeta.setDegraded(2, false);
    Assert.assertEquals(Boolean.FALSE, temporaryMeta.getGlobalDegraded());

    temporaryMeta.setDegraded(2, true);
    Assert.assertEquals(Boolean.TRUE, temporaryMeta.getGlobalDegraded());

    temporaryMeta.setDegraded(2, null);
    Assert.assertEquals(Boolean.FALSE, temporaryMeta.getGlobalDegraded());

    temporaryMeta.setDegraded(1, null);
    Assert.assertNull(temporaryMeta.getGlobalDegraded());
  }
}
