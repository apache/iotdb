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
package org.apache.iotdb.db.storageengine;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetConfigurationReq;
import org.apache.iotdb.commons.conf.EditionGate;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

public class StorageEngineSetConfigProGateTest {

  @After
  public void tearDown() {
    EditionGate.clearProOverrideForTest();
  }

  @Test
  public void proRejectsGatedKey() {
    EditionGate.setProOverrideForTest(true);
    Map<String, String> cfg = new HashMap<>();
    cfg.put("enable_white_list", "true");
    TSetConfigurationReq req = new TSetConfigurationReq();
    req.setConfigs(cfg);
    TSStatus status = StorageEngine.getInstance().setConfiguration(req);
    assertNotEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }

  @Test
  public void proAllowsNonGatedKey() {
    // A non-gated key must NOT be rejected by the edition gate. The request may fail later for
    // unrelated reasons (e.g. missing config file), but that failure must not be the gated-key
    // rejection, so we assert the returned message never mentions the edition rejection text.
    EditionGate.setProOverrideForTest(true);
    Map<String, String> cfg = new HashMap<>();
    cfg.put("some_non_gated_key", "1");
    TSetConfigurationReq req = new TSetConfigurationReq();
    req.setConfigs(cfg);
    TSStatus status = StorageEngine.getInstance().setConfiguration(req);
    String msg = status.getMessage() == null ? "" : status.getMessage();
    assertFalse(msg.contains("is not available in this edition"));
  }
}
