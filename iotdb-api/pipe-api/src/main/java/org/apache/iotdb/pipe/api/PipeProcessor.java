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
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

/**
 * PipeProcessor
 *
 * <p>PipeProcessor is used to filter and transform the Event formed by the PipeExtractor.
 *
 * <p>The lifecycle of a PipeProcessor is as follows:
 *
 * <ul>
 *   <li>When a collaboration task is created, the KV pairs of `WITH PROCESSOR` clause in SQL are
 *       parsed and the validation method {@link PipeProcessor#validate(PipeParameterValidator)}
 *       will be called to validate the parameters.
 *   <li>Before the collaboration task starts, the method {@link
 *       PipeProcessor#customize(PipeParameters, PipeProcessorRuntimeConfiguration)} will be called
 *       to config the runtime behavior of the PipeProcessor.
 *   <li>While the collaboration task is in progress:
 *       <ul>
 *         <li>PipeExtractor captures the events and wraps them into three types of Event instances.
 *         <li>PipeProcessor processes the event and then passes them to the PipeConnector. The
 *             following 3 methods will be called: {@link
 *             PipeProcessor#process(TabletInsertionEvent, EventCollector)}, {@link
 *             PipeProcessor#process(TsFileInsertionEvent, EventCollector)} and {@link
 *             PipeProcessor#process(Event, EventCollector)}.
 *         <li>PipeConnector serializes the events into binaries and send them to sinks.
 *       </ul>
 *   <li>When the collaboration task is cancelled (the `DROP PIPE` command is executed), the {@link
 *       PipeProcessor#close() } method will be called.
 * </ul>
 */
public interface PipeProcessor extends PipePlugin {

  /**
   * This method is mainly used to validate {@link PipeParameters} and it is executed before {@link
   * PipeProcessor#customize(PipeParameters, PipeProcessorRuntimeConfiguration)} is called.
   *
   * @param validator the validator used to validate {@link PipeParameters}
   * @throws Exception if any parameter is not valid
   */
  void validate(PipeParameterValidator validator) throws Exception;

  /**
   * This method is mainly used to customize PipeProcessor. In this method, the user can do the
   * following things:
   *
   * <ul>
   *   <li>Use PipeParameters to parse key-value pair attributes entered by the user.
   *   <li>Set the running configurations in PipeProcessorRuntimeConfiguration.
   * </ul>
   *
   * <p>This method is called after the method {@link
   * PipeProcessor#validate(PipeParameterValidator)} is called and before the beginning of the
   * events processing.
   *
   * @param parameters used to parse the input parameters entered by the user
   * @param configuration used to set the required properties of the running PipeProcessor
   * @throws Exception the user can throw errors if necessary
   */
  void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration)
      throws Exception;

  /**
   * This method is called to process the TabletInsertionEvent.
   *
   * @param tabletInsertionEvent TabletInsertionEvent to be processed
   * @param eventCollector used to collect result events after processing
   * @throws Exception the user can throw errors if necessary
   */
  void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws Exception;

  /**
   * This method is called to process the TsFileInsertionEvent.
   *
   * @param tsFileInsertionEvent TsFileInsertionEvent to be processed
   * @param eventCollector used to collect result events after processing
   * @throws Exception the user can throw errors if necessary
   */
  default void process(TsFileInsertionEvent tsFileInsertionEvent, EventCollector eventCollector)
      throws Exception {
    for (final TabletInsertionEvent tabletInsertionEvent :
        tsFileInsertionEvent.toTabletInsertionEvents()) {
      process(tabletInsertionEvent, eventCollector);
    }
  }

  /**
   * This method is called to process the Event.
   *
   * @param event Event to be processed
   * @param eventCollector used to collect result events after processing
   * @throws Exception the user can throw errors if necessary
   */
  void process(Event event, EventCollector eventCollector) throws Exception;
}
