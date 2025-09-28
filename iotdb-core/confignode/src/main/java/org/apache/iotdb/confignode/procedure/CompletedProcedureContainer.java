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
1package org.apache.iotdb.confignode.procedure;
1
1public class CompletedProcedureContainer<Env> {
1  private final Procedure<Env> procedure;
1
1  public CompletedProcedureContainer(Procedure<Env> procedure) {
1    this.procedure = procedure;
1  }
1
1  public Procedure<Env> getProcedure() {
1    return procedure;
1  }
1
1  public boolean isExpired(long now, long evictTtl) {
1    return (now - procedure.getLastUpdate()) >= evictTtl;
1  }
1}
1