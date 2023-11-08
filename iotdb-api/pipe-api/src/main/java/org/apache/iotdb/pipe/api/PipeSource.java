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

package org.apache.iotdb.pipe.api;

import org.apache.iotdb.pipe.api.customizer.configuration.PipeSourceRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

/**
 * PipeSource
 *
 * <p>PipeSource is responsible for capturing events from sources.
 *
 * <p>Various data sources can be supported by implementing different PipeSource classes.
 *
 * <p>The lifecycle of a PipeSource is as follows:
 *
 * <ul>
 *   <li>When a collaboration task is created, the KV pairs of `WITH EXTRACTOR` clause in SQL are
 *       parsed and the validation method {@link PipeSource#validate(PipeParameterValidator)} will
 *       be called to validate the parameters.
 *   <li>Before the collaboration task starts, the method {@link
 *       PipeSource#customize(PipeParameters, PipeSourceRuntimeConfiguration)} will be called to
 *       config the runtime behavior of the PipeSource.
 *   <li>Then the method {@link PipeSource#start()} will be called to start the PipeSource.
 *   <li>While the collaboration task is in progress, the method {@link PipeSource#supply()} will be
 *       called to capture events from sources and then the events will be passed to the
 *       PipeProcessor.
 *   <li>The method {@link PipeSource#close()} will be called when the collaboration task is
 *       cancelled (the `DROP PIPE` command is executed).
 * </ul>
 */
public interface PipeSource extends PipeExtractor {

  /**
   * This method is mainly used to validate {@link PipeParameters} and it is executed before {@link
   * PipeSource#customize(PipeParameters, PipeSourceRuntimeConfiguration)} is called.
   *
   * @param validator the validator used to validate {@link PipeParameters}
   * @throws Exception if any parameter is not valid
   */
  void validate(PipeParameterValidator validator) throws Exception;

  /**
   * This method is mainly used to customize PipeSource. In this method, the user can do the
   * following things:
   *
   * <ul>
   *   <li>Use PipeParameters to parse key-value pair attributes entered by the user.
   *   <li>Set the running configurations in PipeSourceRuntimeConfiguration.
   * </ul>
   *
   * <p>This method is called after the method {@link PipeSource#validate(PipeParameterValidator)}
   * is called.
   *
   * @param parameters used to parse the input parameters entered by the user
   * @param configuration used to set the required properties of the running PipeSource
   * @throws Exception the user can throw errors if necessary
   */
  void customize(PipeParameters parameters, PipeSourceRuntimeConfiguration configuration)
      throws Exception;

  /**
   * Start the extractor. After this method is called, events should be ready to be supplied by
   * {@link PipeSource#supply()}. This method is called after {@link
   * PipeSource#customize(PipeParameters, PipeSourceRuntimeConfiguration)} is called.
   *
   * @throws Exception the user can throw errors if necessary
   */
  void start() throws Exception;

  /**
   * Supply single event from the extractor and the caller will send the event to the processor.
   * This method is called after {@link PipeSource#start()} is called.
   *
   * @return the event to be supplied. the event may be null if the extractor has no more events at
   *     the moment, but the extractor is still running for more events.
   * @throws Exception the user can throw errors if necessary
   */
  Event supply() throws Exception;
}
