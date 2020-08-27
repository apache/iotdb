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

import java.util.List;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.customizer.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.UDTFConfigurations;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class UDTFExecutor extends UDFExecutor {

  protected UDTFConfigurations configurations;

  public UDTFExecutor(UDFContext context) throws QueryProcessException {
    super(context);
  }

  @Override
  public void initializeUDF() throws QueryProcessException {
    parameters = new UDFParameters(context.getPaths(), context.getAttributes());
    configurations = new UDTFConfigurations(context.getPaths());
    ((UDTF) udf).initializeUDF(parameters, configurations);
    configurations.check();
    isInitialized = true;
  }

  public UDTFConfigurations getConfigurations() throws QueryProcessException {
    if (!isInitialized) {
      throw new QueryProcessException("UDTF Executor is not initialized.");
    }
    return configurations;
  }

  public List<TSDataType> getResultTypes() throws QueryProcessException {
    return getConfigurations().getOutputDataTypes();
  }
}
