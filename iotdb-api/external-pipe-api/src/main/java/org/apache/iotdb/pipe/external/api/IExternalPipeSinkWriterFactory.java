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

package org.apache.iotdb.pipe.external.api;

import java.util.Map;
import java.util.function.Supplier;

/** Responsible for creating {@link IExternalPipeSinkWriter} with the sink configuration. */
public interface IExternalPipeSinkWriterFactory extends Supplier<IExternalPipeSinkWriter> {
  /**
   * Get the External PIPE's provider info.
   *
   * @return External PIPE s provider name
   */
  String getProviderName();

  /**
   * Get the External PIPE's type name. For example: If customer self-defined getExternalPipeType()
   * return "mySink", corresponding input CMD should be "CREATE PIPESINK mySink1 AS mySink (...)".
   * Otherwise, the CMD will be refused. *
   *
   * @return External PIPE s type name
   */
  String getExternalPipeType();

  /**
   * This function is used to validate the parameters in client CMD. For example: When customer
   * input CMD: "CREATE PIPESINK mySink1 AS mySink (p1='111', p2='abc')", The parameters (p1=111,
   * p2=abc) will be saved in sinkParams and then send it to validateSinkParams(sinkParams) for
   * validation. If validateSinkParams() does not return Exception, the CMD will be processed.
   * Otherwise, the CMD will be refused with prompt info that is from Exception.getMessage();
   *
   * @param sinkParams Contains the parameters in CMD "CREATE PIPESINK ..."
   * @return true means successful
   * @throws Exception
   */
  void validateSinkParams(Map<String, String> sinkParams) throws Exception;

  /**
   * Initialize with the configuration of the corresponding sink, which may contain information to
   * set up a connection to the third-party system.
   *
   * @param sinkParams Parameters of the corresponding sink.
   */
  void initialize(Map<String, String> sinkParams) throws Exception;
}
