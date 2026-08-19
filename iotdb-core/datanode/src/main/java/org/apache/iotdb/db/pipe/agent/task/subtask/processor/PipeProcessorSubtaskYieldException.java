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

package org.apache.iotdb.db.pipe.agent.task.subtask.processor;

/** Internal control-flow exception that immediately yields the current processor worker. */
public final class PipeProcessorSubtaskYieldException extends RuntimeException {

  private static final PipeProcessorSubtaskYieldException PAUSE_REQUESTED_INSTANCE =
      new PipeProcessorSubtaskYieldException(Reason.PAUSE_REQUESTED);
  private static final PipeProcessorSubtaskYieldException PARSER_NOT_ADMITTED_INSTANCE =
      new PipeProcessorSubtaskYieldException(Reason.PARSER_NOT_ADMITTED);

  private final Reason reason;

  private PipeProcessorSubtaskYieldException(final Reason reason) {
    super(null, null, false, false);
    this.reason = reason;
  }

  public static PipeProcessorSubtaskYieldException pauseRequested() {
    return PAUSE_REQUESTED_INSTANCE;
  }

  public static PipeProcessorSubtaskYieldException parserNotAdmitted() {
    return PARSER_NOT_ADMITTED_INSTANCE;
  }

  public Reason getReason() {
    return reason;
  }

  public enum Reason {
    PAUSE_REQUESTED,
    PARSER_NOT_ADMITTED
  }
}
