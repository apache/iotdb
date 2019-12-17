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
package org.apache.iotdb.cluster;

import java.io.IOException;
import org.apache.iotdb.cluster.server.MetaClusterServer;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterMain {

  private static final Logger logger = LoggerFactory.getLogger(ClusterMain.class);

  // establish the cluster as a seed
  private static final String MODE_START = "-s";
  // join an established cluster
  private static final String MODE_ADD = "-a";

  public static MetaClusterServer metaServer;

  public static void main(String[] args) throws InterruptedException {
    if (args.length < 1) {
      logger.error("Usage: <start mode>");
      return;
    }
    String mode = args[0];

    try {
      metaServer = new MetaClusterServer();
      metaServer.start();
    } catch (TTransportException | IOException e) {
      logger.error("Cannot set up service", e);
      return;
    }

    logger.info("Running mode {}", mode);
    if (MODE_START.equals(mode)) {
      metaServer.buildCluster();
    } else if (MODE_ADD.equals(mode)) {
      if (!metaServer.joinCluster()) {
        metaServer.stop();
      }
    } else {
      logger.error("Unrecognized mode {}", mode);
    }
  }
}
