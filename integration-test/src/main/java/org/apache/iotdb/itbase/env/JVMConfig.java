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
package org.apache.iotdb.itbase.env;

/** this interface is used to store the JVM parameters of test nodes. */
public interface JVMConfig {

  /**
   * Set the init heap size in MB.
   *
   * @param initSize the initial heap size in MB(passed by -Xms).
   * @return the instance of the config itself.
   */
  JVMConfig setInitHeapSize(int initSize);

  /**
   * Set the max heap size in MB.
   *
   * @param maxSize the max heap size in MB(passed by -Xmx).
   * @return the instance of the config itself.
   */
  JVMConfig setMaxHeapSize(int maxSize);

  /**
   * Set the max memory size allocated out of heap in MB.
   *
   * @param maxSize the max heap size in MB(passed by -XX:MaxDirectMemorySize).
   * @return the instance of the config itself.
   */
  JVMConfig setMaxDirectMemorySize(int maxSize);
}
