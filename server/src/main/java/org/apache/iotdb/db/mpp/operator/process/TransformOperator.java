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

package org.apache.iotdb.db.mpp.operator.process;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.udf.core.executor.UDTFContext;
import org.apache.iotdb.db.query.udf.core.layer.EvaluationDAGBuilder;
import org.apache.iotdb.db.query.udf.core.layer.RawQueryInputLayer;
import org.apache.iotdb.db.query.udf.core.layer.TsBlockInputDataSet;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.db.query.udf.service.UDFRegistrationService;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import java.io.IOException;
import java.util.List;

public class TransformOperator implements ProcessOperator {

  protected static final float UDF_READER_MEMORY_BUDGET_IN_MB =
      IoTDBDescriptor.getInstance().getConfig().getUdfReaderMemoryBudgetInMB();
  protected static final float UDF_TRANSFORMER_MEMORY_BUDGET_IN_MB =
      IoTDBDescriptor.getInstance().getConfig().getUdfTransformerMemoryBudgetInMB();
  protected static final float UDF_COLLECTOR_MEMORY_BUDGET_IN_MB =
      IoTDBDescriptor.getInstance().getConfig().getUdfCollectorMemoryBudgetInMB();

  private final OperatorContext operatorContext;

  private final Expression[] outputExpressions;
  private final UDTFContext udtfContext;

  private LayerPointReader[] transformers;

  public TransformOperator(
      OperatorContext operatorContext,
      List<TSDataType> inputDataTypes,
      Expression[] outputExpressions,
      UDTFContext udtfContext)
      throws QueryProcessException, IOException {
    this.operatorContext = operatorContext;

    this.outputExpressions = outputExpressions;
    this.udtfContext = udtfContext;

    initTransformers(inputDataTypes);
  }

  protected void initTransformers(List<TSDataType> inputDataTypes)
      throws QueryProcessException, IOException {
    UDFRegistrationService.getInstance().acquireRegistrationLock();
    // This statement must be surrounded by the registration lock.
    UDFClassLoaderManager.getInstance().initializeUDFQuery(operatorContext.getOperatorId());
    try {
      // UDF executors will be initialized at the same time
      transformers =
          new EvaluationDAGBuilder(
                  operatorContext.getOperatorId(),
                  new RawQueryInputLayer(
                      operatorContext.getOperatorId(),
                      UDF_READER_MEMORY_BUDGET_IN_MB,
                      new TsBlockInputDataSet(this, inputDataTypes)),
                  outputExpressions,
                  udtfContext,
                  UDF_TRANSFORMER_MEMORY_BUDGET_IN_MB + UDF_COLLECTOR_MEMORY_BUDGET_IN_MB)
              .buildLayerMemoryAssigner()
              .buildResultColumnPointReaders()
              .getOutputPointReaders();
    } finally {
      UDFRegistrationService.getInstance().releaseRegistrationLock();
    }
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    return null;
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public void close() throws Exception {
    udtfContext.finalizeUDFExecutors(operatorContext.getOperatorId());
  }

  @Override
  public boolean isFinished() throws IOException {
    return false;
  }
}
