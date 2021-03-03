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

package org.apache.iotdb.db.engine.trigger.service;

import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.writelog.io.SingleFileLogReader;

import java.io.File;
import java.io.IOException;

public class TriggerLogReader implements AutoCloseable {

  private final SingleFileLogReader logReader;

  public TriggerLogReader(File logFile) throws IOException {
    logReader = new SingleFileLogReader(logFile);
  }

  public boolean hasNext() {
    return !logReader.isFileCorrupted() && logReader.hasNext();
  }

  public PhysicalPlan next() {
    return logReader.next();
  }

  @Override
  public void close() {
    logReader.close();
  }
}
