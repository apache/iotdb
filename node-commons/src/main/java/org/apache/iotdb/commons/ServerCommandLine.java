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
package org.apache.iotdb.commons;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ServerCommandLine {
  private static final Logger LOG = LoggerFactory.getLogger(ServerCommandLine.class);

  /**
   * Implementing subclasses should return a usage string to print out
   *
   * @return usage
   */
  protected abstract String getUsage();

  /**
   * run command
   *
   * @param args system args
   * @return return 0 if exec success
   */
  protected abstract int run(String[] args) throws Exception;

  protected void usage(String message) {
    if (message != null) {
      System.err.println(message);
      System.err.println();
    }

    System.err.println(getUsage());
  }

  /**
   * Parse and run the given command line.
   *
   * @param args system args
   */
  public void doMain(String[] args) {
    try {
      int result = run(args);
      if (result != 0) {
        System.exit(result);
      }
    } catch (Exception e) {
      LOG.error("Failed to execute system command", e);
      System.exit(-1);
    }
  }
}
