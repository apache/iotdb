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

package org.apache.iotdb.db.sync.externalpipe;

import org.apache.iotdb.pipe.external.api.ExternalPipeSinkWriterStatus;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Represents the status of an external pipe. */
public class ExternalPipeStatus {
  boolean alive;
  List<ExternalPipeSinkWriterStatus> writerStatuses;
  Map<String, Map<String, AtomicInteger>> writerInvocationFailures;

  public void setAlive(boolean alive) {
    this.alive = alive;
  }

  public boolean isAlive() {
    return alive;
  }

  public void setWriterStatuses(List<ExternalPipeSinkWriterStatus> writerStatuses) {
    this.writerStatuses = writerStatuses;
  }

  public List<ExternalPipeSinkWriterStatus> getWriterStatuses() {
    return writerStatuses;
  }

  public void setWriterInvocationFailures(
      Map<String, Map<String, AtomicInteger>> writerInvocationFailures) {
    this.writerInvocationFailures = writerInvocationFailures;
  }

  public Map<String, Map<String, AtomicInteger>> getWriterInvocationFailures() {
    return writerInvocationFailures;
  }
}
