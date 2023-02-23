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

import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.customizer.config.ProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.paramater.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.paramater.PipeValidator;
import org.apache.iotdb.pipe.api.event.DeletionEvent;
import org.apache.iotdb.pipe.api.event.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.TsFileInsertionEvent;

/**
 * PipeProcessor
 *
 * <p>PipeProcessor is used to filter and transform the Event formed by the PipeCollector.
 *
 * <p>The lifecycle of a PipeProcessor is as follows:
 *
 * <ul>
 *   <li>Before the sync task starts, the KV pair of `WITH PROCESSOR` clause in SQL is parsed and
 *       the validate method {@link PipeProcessor#validate(PipeValidator)}is called to validate the
 *       parameters.
 *   <li>When the sync task starts, load and initialize the PipeProcessor instance, and then call
 *       the beforeStart method {@link PipeProcessor#beforeStart(PipeParameters,
 *       ProcessorRuntimeConfiguration)}
 *   <li>while the sync task is in progress
 *       <ul>
 *         <li>PipeCollector capture the writes and deletes events and generates three types of
 *             Event
 *         <li>the Event is delivered to the corresponding process method in PipeProcessor
 *         <li>the Event is delivered to the PipeConnector after processing is completed
 *       </ul>
 *   <li>When the sync task is cancelled(When `DROP PIPE` is executed), the PipeProcessor calls the
 *       autoClose method.
 * </ul>
 */
public interface PipeProcessor extends AutoCloseable {

  /**
   * This method is mainly used to validate {@link PipeParameters} and it is executed before {@link
   * PipeProcessor#beforeStart(PipeParameters, ProcessorRuntimeConfiguration)} is called.
   *
   * @param validator the validator used to validate {@link PipeParameters}
   * @throws Exception if any parameter is not valid
   */
  void validate(PipeValidator validator) throws Exception;

  /**
   * This method is mainly used to customize PipeProcessor. In this method, the user can do the
   * following things:
   *
   * <ul>
   *   <li>Use PipeParameters to parse key-value pair attributes entered by the user.
   *   <li>Set the running configurations in ProcessorRuntimeConfiguration.
   * </ul>
   *
   * <p>This method is called after the PipeProcessor is instantiated and before the beginning of
   * the data processing.
   *
   * @param params used to parse the input parameters entered by the user
   * @param configs used to set the required properties of the running PipeProcessor
   * @throws Exception the user can throw errors if necessary
   */
  void beforeStart(PipeParameters params, ProcessorRuntimeConfiguration configs) throws Exception;

  /**
   * This method is called to process the TabletInsertionEvent.
   *
   * @param te the insertion event of Tablet
   * @param ec used to collect output data events
   * @throws Exception the user can throw errors if necessary
   */
  void process(TabletInsertionEvent te, EventCollector ec) throws Exception;

  /**
   * This method is called to process the TsFileInsertionEvent.
   *
   * @param te the insertion event of TsFile
   * @param ec used to collect output data events
   * @throws Exception the user can throw errors if necessary
   */
  void process(TsFileInsertionEvent te, EventCollector ec) throws Exception;

  /**
   * This method is called to process the DeletionEvent.
   *
   * @param de the event of Deletion
   * @param ec used to collect output data events
   * @throws Exception the user can throw errors if necessary
   */
  void process(DeletionEvent de, EventCollector ec) throws Exception;
}
