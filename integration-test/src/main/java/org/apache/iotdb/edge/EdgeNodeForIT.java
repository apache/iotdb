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

package org.apache.iotdb.edge;

import org.apache.iotdb.confignode.service.ConfigNodeForOtherIT;
import org.apache.iotdb.db.HackTimechoServer;

/**
 * Integration-test entry point for the Edge distribution zip. Mirrors how the template-node ITs
 * launch their nodes: the ConfigNode part boots {@link ConfigNodeForOtherIT} (whose
 * RegulateManagerWithoutAnyLimit drops every activation/license limit) and the DataNode part boots
 * {@link HackTimechoServer}, both through the shared {@link EdgeNode} orchestration so the merged
 * process behaves exactly like the product entry.
 *
 * <p>Not shipped anywhere: {@code IoTDBEdgeBasicIT} drops this module's jar into the extracted
 * zip's {@code lib/} and points {@code start-edge.sh}/{@code stop-edge.sh} at this class via the
 * {@code EDGE_MAIN_CLASS} environment variable. This requires the {@code with-integration-tests}
 * build, whose {@code proguard-it.conf} keeps the {@code com.timecho.iotdb.manager.**} and {@code
 * org.apache.**} names linkable from unobfuscated classes.
 */
public final class EdgeNodeForIT {

  private EdgeNodeForIT() {}

  public static void main(String[] args) throws Exception {
    EdgeNode.start(
        () -> ConfigNodeForOtherIT.main(new String[] {"-s"}),
        () -> HackTimechoServer.main(new String[] {"-s"}));
  }
}
