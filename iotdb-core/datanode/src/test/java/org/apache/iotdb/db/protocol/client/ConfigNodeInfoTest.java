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

package org.apache.iotdb.db.protocol.client;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.file.SystemPropertiesHandler;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConfigNodeInfoTest {

  @Before
  public void setUp() {
    ConfigNodeInfo.reinitializeStatics();
    ConfigNodeInfo.getInstance().systemPropertiesHandler = new NoopSystemPropertiesHandler();
  }

  @Test
  public void testUpdateConfigNodeLocationsCachesNodeIds() {
    final ConfigNodeInfo configNodeInfo = ConfigNodeInfo.getInstance();
    final TEndPoint firstInternalEndPoint = new TEndPoint("127.0.0.1", 10710);
    final TEndPoint secondInternalEndPoint = new TEndPoint("127.0.0.2", 10710);

    assertTrue(
        configNodeInfo.updateConfigNodeLocations(
            Arrays.asList(
                new TConfigNodeLocation(
                    1, firstInternalEndPoint, new TEndPoint("127.0.0.1", 10720)),
                new TConfigNodeLocation(
                    2, secondInternalEndPoint, new TEndPoint("127.0.0.2", 10720)))));
    assertEquals(1, configNodeInfo.getConfigNodeId(new TEndPoint("127.0.0.1", 10710)));
    assertEquals(2, configNodeInfo.getConfigNodeId(new TEndPoint("127.0.0.2", 10710)));

    assertTrue(
        configNodeInfo.updateConfigNodeList(Collections.singletonList(firstInternalEndPoint)));
    assertEquals(1, configNodeInfo.getConfigNodeId(new TEndPoint("127.0.0.1", 10710)));
    assertEquals(-1, configNodeInfo.getConfigNodeId(new TEndPoint("127.0.0.2", 10710)));
  }

  private static class NoopSystemPropertiesHandler extends SystemPropertiesHandler {

    private NoopSystemPropertiesHandler() {
      super("target/noop-system.properties");
    }

    @Override
    public boolean fileExist() {
      return false;
    }
  }
}
