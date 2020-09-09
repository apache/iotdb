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

package org.apache.iotdb.db.query.udf.core;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.exception.query.QueryProcessException;

public abstract class UDFExecutor {

  protected final UDFContext context;

  protected List<Integer> pathIndex2DeduplicatedPathIndex;

  public UDFExecutor(UDFContext context) {
    this.context = context;
    pathIndex2DeduplicatedPathIndex = new ArrayList<>();
  }

  abstract public void initializeUDF() throws QueryProcessException;

  abstract public void executeUDF() throws QueryProcessException;

  abstract public void finalizeUDF();

  public void addPathIndex2DeduplicatedPathIndex(int index) {
    pathIndex2DeduplicatedPathIndex.add(index);
  }

  public UDFContext getContext() {
    return context;
  }
}
