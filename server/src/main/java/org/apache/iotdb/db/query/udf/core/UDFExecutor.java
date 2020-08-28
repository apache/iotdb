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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.api.UDF;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.service.UDFRegistrationService;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

public abstract class UDFExecutor {

  protected final UDFContext context;
  protected final UDF udf;
  protected final List<String> uniqueColumnNamesForReaderDeduplication;
  protected boolean isInitialized;

  protected UDFParameters parameters;

  protected Map<Path, Integer> path2DeduplicatedPathIndex;

  public UDFExecutor(UDFContext context) throws QueryProcessException {
    this.context = context;
    udf = UDFRegistrationService.getInstance().reflect(context);
    uniqueColumnNamesForReaderDeduplication = new ArrayList<>();
    for (Path path : context.getPaths()) {
      uniqueColumnNamesForReaderDeduplication
          .add(context.getName() + "(" + path.getFullPath() + ")");
    }
    isInitialized = false;
    path2DeduplicatedPathIndex = new HashMap<>();
  }

  abstract public void initializeUDF() throws QueryProcessException;

  abstract public TSDataType getOutputDataType() throws QueryProcessException;

  public void finalizeUDF() {
    udf.finalizeUDF();
  }

  public boolean isInitialized() {
    return isInitialized;
  }

  public void addPath2DeduplicatedPathIndex(Path path, int index) {
    path2DeduplicatedPathIndex.put(path, index);
  }

  public UDFContext getContext() {
    return context;
  }

  public String getUniqueColumnNamesForReaderDeduplication(int index) {
    return uniqueColumnNamesForReaderDeduplication.get(index);
  }
}
