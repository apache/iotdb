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
package org.apache.iotdb.commons.utils;

import java.lang.management.ManagementFactory;

public class ProcessIdUtils {
  /**
   * There exists no platform-independent way that can be guaranteed to work in all jvm
   * implementations. ManagementFactory.getRuntimeMXBean().getName() looks like the best solution,
   * and typically includes the PID. On linux+windows, it returns a value like "12345@hostname"
   * (12345 being the process id).
   *
   * @return process id of running Java virtual machine
   */
  public static String getProcessId() {
    return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
  }
}
