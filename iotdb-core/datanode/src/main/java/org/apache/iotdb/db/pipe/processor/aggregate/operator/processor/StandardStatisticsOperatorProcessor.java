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

import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.AggregatedResultOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.standardstatistics.AverageOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.standardstatistics.ClearanceFactorOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.standardstatistics.CrestFactorOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.standardstatistics.FormFactorOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.standardstatistics.KurtosisOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.standardstatistics.MaxValueOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.standardstatistics.MinValueOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.standardstatistics.PeakOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.standardstatistics.PulseFactorOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.standardstatistics.RootMeanSquareOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.standardstatistics.SkewnessOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.standardstatistics.VarianceOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.IntermediateResultOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.sametype.numeric.AbsoluteMaxOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.sametype.numeric.IntegralPoweredSumOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.sametype.numeric.MaxOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.sametype.numeric.MinOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.specifictype.doubletype.FractionPoweredSumOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.specifictype.integertype.CountOperator;
import org.apache.iotdb.pipe.api.annotation.TreeModel;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

@TreeModel
public class StandardStatisticsOperatorProcessor extends AbstractOperatorProcessor {
  @Override
  public Set<AggregatedResultOperator> getAggregatorOperatorSet() {
    return Collections.unmodifiableSet(
        new HashSet<>(
            Arrays.asList(
                new AverageOperator(),
                new ClearanceFactorOperator(),
                new CrestFactorOperator(),
                new FormFactorOperator(),
                new KurtosisOperator(),
                new PeakOperator(),
                new MaxValueOperator(),
                new MinValueOperator(),
                new PulseFactorOperator(),
                new RootMeanSquareOperator(),
                new SkewnessOperator(),
                new VarianceOperator())));
  }

  @Override
  public Set<Supplier<IntermediateResultOperator>> getIntermediateResultOperatorSupplierSet() {
    return Collections.unmodifiableSet(
        new HashSet<>(
            Arrays.asList(
                AbsoluteMaxOperator::new,
                CountOperator::new,
                () -> new FractionPoweredSumOperator(0.5),
                () -> new IntegralPoweredSumOperator(1),
                () -> new IntegralPoweredSumOperator(2),
                () -> new IntegralPoweredSumOperator(3),
                () -> new IntegralPoweredSumOperator(4),
                MaxOperator::new,
                MinOperator::new)));
  }
}
