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

package org.apache.iotdb.consensus.pipe.consensuspipe;

import java.util.Map;

public interface ConsensusPipeDispatcher {
  void createPipe(
      String pipeName,
      Map<String, String> extractorAttributes,
      Map<String, String> processorAttributes,
      Map<String, String> connectorAttributes,
      boolean needManuallyStart)
      throws Exception;

  void startPipe(String pipeName) throws Exception;

  void stopPipe(String pipeName) throws Exception;

  /**
   * Use ConsensusPipeName instead of String to provide information for receiverAgent to release
   * corresponding resource
   */
  void dropPipe(ConsensusPipeName pipeName) throws Exception;
}
