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

package org.apache.iotdb.db;

import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;

import com.timecho.iotdb.DataNode;

/** This class is used to run integration test using timecho-server without license. */
public class HackTimechoServer extends DataNode {
  private static final int CPU_CORE_NUM_FOR_TEST = 1;
  private static final long TOTAL_MEMORY_FOR_TEST = 8000000000L;

  public static void main(String[] args) {
    DataNode.prepare();

    HackTimechoServer hackTimechoServer = new HackTimechoServer();
    int returnCode = hackTimechoServer.run(args);
    if (returnCode != 0) {
      System.exit(returnCode);
    }
  }

  // Fix the number of CPU cores for testing purposes
  @Override
  public TDataNodeConfiguration generateDataNodeConfiguration() {
    // Set DataNodeLocation
    TDataNodeLocation location = generateDataNodeLocation();
    // Set NodeResource
    TNodeResource resource = new TNodeResource();
    resource.setCpuCoreNum(CPU_CORE_NUM_FOR_TEST);
    resource.setMaxMemory(TOTAL_MEMORY_FOR_TEST);
    return new TDataNodeConfiguration(location, resource);
  }

  private static class HackTimechoServerNewHolder {
    private static final HackTimechoServer INSTANCE = new HackTimechoServer();

    private HackTimechoServerNewHolder() {}
  }

  public static HackTimechoServer getInstance() {
    return HackTimechoServer.HackTimechoServerNewHolder.INSTANCE;
  }
}
