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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.transformation.api.LayerRowReader;
import org.apache.iotdb.db.mpp.transformation.api.YieldableState;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;

import java.io.IOException;

public class UDFQueryRowTransformer extends UDFQueryTransformer {

  protected final LayerRowReader layerRowReader;

  public UDFQueryRowTransformer(LayerRowReader layerRowReader, UDTFExecutor executor) {
    super(executor);
    this.layerRowReader = layerRowReader;
  }

  @Override
  protected YieldableState tryExecuteUDFOnce() throws QueryProcessException, IOException {
    final YieldableState yieldableState = layerRowReader.yield();
    if (yieldableState != YieldableState.YIELDABLE) {
      return yieldableState;
    }
    if (layerRowReader.isCurrentNull()) {
      currentNull = true;
    }
    executor.execute(layerRowReader.currentRow(), currentNull);
    layerRowReader.readyForNext();
    return YieldableState.YIELDABLE;
  }

  @Override
  protected boolean executeUDFOnce() throws IOException, QueryProcessException {
    if (!layerRowReader.next()) {
      return false;
    }
    if (layerRowReader.isCurrentNull()) {
      currentNull = true;
    }
    executor.execute(layerRowReader.currentRow(), currentNull);
    layerRowReader.readyForNext();
    return true;
  }
}
