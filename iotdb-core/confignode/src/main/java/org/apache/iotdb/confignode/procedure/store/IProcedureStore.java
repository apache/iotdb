1/*
1 * Licensed to the Apache Software Foundation (ASF) under one
1 * or more contributor license agreements.  See the NOTICE file
1 * distributed with this work for additional information
1 * regarding copyright ownership.  The ASF licenses this file
1 * to you under the Apache License, Version 2.0 (the
1 * "License"); you may not use this file except in compliance
1 * with the License.  You may obtain a copy of the License at
1 *
1 *     http://www.apache.org/licenses/LICENSE-2.0
1 *
1 * Unless required by applicable law or agreed to in writing,
1 * software distributed under the License is distributed on an
1 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1 * KIND, either express or implied.  See the License for the
1 * specific language governing permissions and limitations
1 * under the License.
1 */
1
1package org.apache.iotdb.confignode.procedure.store;
1
1import org.apache.iotdb.confignode.persistence.ProcedureInfo;
1import org.apache.iotdb.confignode.procedure.Procedure;
1
1import java.util.List;
1
1public interface IProcedureStore<Env> {
1
1  boolean isRunning();
1
1  void setRunning(boolean running);
1
1  List<Procedure<Env>> load();
1
1  List<Procedure<Env>> getProcedures();
1
1  ProcedureInfo getProcedureInfo();
1
1  long getNextProcId();
1
1  void update(Procedure<Env> procedure);
1
1  void update(Procedure<Env>[] subprocs);
1
1  void delete(long procId);
1
1  void delete(long[] childProcIds);
1
1  void delete(long[] batchIds, int startIndex, int batchCount);
1
1  void cleanup();
1
1  void stop();
1
1  void start();
1
1  boolean isOldVersionProcedureStore();
1}
1