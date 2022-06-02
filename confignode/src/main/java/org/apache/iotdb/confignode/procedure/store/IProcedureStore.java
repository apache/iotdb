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

package org.apache.iotdb.confignode.procedure.store;

import org.apache.iotdb.confignode.procedure.Procedure;

import java.util.List;

public interface IProcedureStore {

  boolean isRunning();

  void setRunning(boolean running);

  void load(List<Procedure> procedureList);

  void update(Procedure procedure);

  void update(Procedure[] subprocs);

  void delete(long procId);

  void delete(long[] childProcIds);

  void delete(long[] batchIds, int startIndex, int batchCount);

  void cleanup();

  void stop();

  void start();
}
