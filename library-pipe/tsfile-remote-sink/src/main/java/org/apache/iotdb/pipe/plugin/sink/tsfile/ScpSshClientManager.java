/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.pipe.plugin.sink.tsfile;

import org.apache.sshd.client.SshClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

class ScpSshClientManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScpSshClientManager.class);

  private static volatile SshClient client;

  private ScpSshClientManager() {}

  static SshClient getClient() throws IOException {
    if (client == null || !client.isStarted()) {
      synchronized (ScpSshClientManager.class) {
        if (client == null || !client.isStarted()) {
          client = createClient();
          LOGGER.info("Created static shared SCP SSH client");
        }
      }
    }
    return client;
  }

  private static SshClient createClient() throws IOException {
    try {
      System.setProperty("org.apache.sshd.security.provider.BC.enabled", "false");
      final SshClient sshClient = SshClient.setUpDefaultClient();
      sshClient.start();
      return sshClient;
    } catch (Exception e) {
      throw new IOException("Failed to create shared SCP SSH client", e);
    }
  }
}
