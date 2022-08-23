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

package org.apache.iotdb.db.mpp.transformation.dag.transformer.multi;

import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.Transformer;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public abstract class UDFQueryTransformer extends Transformer {

  protected final UDTFExecutor executor;

  protected TSDataType tsDataType;

  protected boolean terminated;

  protected UDFQueryTransformer(UDTFExecutor executor) {
    this.executor = executor;
    tsDataType =
        UDFDataTypeTransformer.transformToTsDataType(
            executor.getConfigurations().getOutputDataType());
    terminated = false;
  }

  protected final boolean terminate() throws QueryProcessException {
    if (terminated) {
      return false;
    }
    executor.terminate();
    terminated = true;
    return true;
  }

  @Override
  public final TSDataType getDataType() {
    return tsDataType;
  }
}
