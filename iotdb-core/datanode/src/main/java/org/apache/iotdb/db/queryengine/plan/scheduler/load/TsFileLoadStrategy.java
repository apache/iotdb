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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;

/**
 * Strategy of the LOAD pipeline for loading one source TsFile. The scheduler ({@link
 * LoadTsFileScheduler}) decides which strategy applies via {@code needDecodeTsFile}; each strategy
 * owns its complete pipeline, phase bookkeeping and phase time metrics.
 *
 * <p>Implementations:
 *
 * <ul>
 *   <li>{@link LocalLoadStrategy} - no decode needed, dispatch the file to the local region;
 *   <li>{@link TwoPhaseConsensusLoadStrategy} - decode, stream PIECE batches through consensus,
 *       then PREPARE+COMMIT or ABORT.
 * </ul>
 */
public interface TsFileLoadStrategy {

  /**
   * Loads the given TsFile.
   *
   * @return true if the file was loaded successfully
   */
  boolean execute(LoadSingleTsFileNode node) throws IoTDBException;
}
