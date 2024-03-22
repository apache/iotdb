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

package org.apache.iotdb.db.pipe.processor.aggregate.operator.processor;

import org.apache.iotdb.db.pipe.processor.aggregate.AbstractFormalProcessor;
import org.apache.iotdb.db.pipe.processor.aggregate.AggregateProcessor;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.AggregatedResultOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.IntermediateResultOperator;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import java.util.Set;
import java.util.function.Supplier;

/**
 * {@link AbstractOperatorProcessor} is the formal processor defining the operators adoptable for
 * {@link AggregateProcessor}.
 */
public abstract class AbstractOperatorProcessor extends AbstractFormalProcessor {
  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    // Do nothing by default
  }

  @Override
  public void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    // Do nothing by default
  }

  @Override
  public void close() throws Exception {
    // Do nothing by default
  }

  /////////////////////////////// Child classes logic ///////////////////////////////

  // Child classes must override these logics to be functional.
  /**
   * Get the supported aggregators and its corresponding {@link AggregatedResultOperator}.
   *
   * @return Map {@literal <}AggregatorName, {@link AggregatedResultOperator}{@literal >}
   */
  public abstract Set<AggregatedResultOperator> getAggregatorOperatorSet();

  /**
   * Get the supported intermediate results and its corresponding {@link
   * IntermediateResultOperator}'s suppliers. The supplier is needed because the operator is
   * stateful and used in abundance.
   *
   * @return Map {@literal <}AggregatorName, {@link IntermediateResultOperator}{@literal >}
   */
  public abstract Set<Supplier<IntermediateResultOperator>>
      getIntermediateResultOperatorSupplierSet();
}
