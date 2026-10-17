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

package org.apache.iotdb.commons.concurrent;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ThreadNameTest {

  @Test
  public void testModuleMappingsRemainAligned() {
    assertEquals(
        ThreadModule.IOT_CONSENSUS,
        ThreadName.getModuleTheThreadBelongs(ThreadName.IOT_CONSENSUS_V2_RPC_SERVICE.getName()));
    assertEquals(
        ThreadModule.RATIS_CONSENSUS, ThreadName.getModuleTheThreadBelongs("1-server-thread"));
    assertEquals(
        ThreadModule.METRICS,
        ThreadName.getModuleTheThreadBelongs(ThreadName.PROMETHEUS_REPORTER_HTTP.getName()));
    assertEquals(
        ThreadModule.RPC,
        ThreadName.getModuleTheThreadBelongs(ThreadName.CONFIGNODE_RPC_SERVICE.getName()));
    assertEquals(
        ThreadModule.OTHER,
        ThreadName.getModuleTheThreadBelongs(ThreadName.ACTIVE_LOAD_TSFILE_LOADER.getName()));
  }
}
