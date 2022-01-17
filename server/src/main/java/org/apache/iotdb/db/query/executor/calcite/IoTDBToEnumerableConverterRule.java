/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.query.executor.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * Rule to convert a relational expression from
 * {@link IoTDBRel#CONVENTION} to {@link EnumerableConvention}.
 */
public class IoTDBToEnumerableConverterRule extends ConverterRule {
  /** Singleton instance of IoTDBToEnumerableConverterRule. */
  public static final ConverterRule INSTANCE = Config.INSTANCE
      .withConversion(RelNode.class, IoTDBRel.CONVENTION,
          EnumerableConvention.INSTANCE, "IoTDBToEnumerableConverterRule")
      .withRuleFactory(IoTDBToEnumerableConverterRule::new)
      .toRule(IoTDBToEnumerableConverterRule.class);

  /** Called from the Config. */
  protected IoTDBToEnumerableConverterRule(Config config) {
    super(config);
  }

  @Override public RelNode convert(RelNode rel) {
    RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
    return new IoTDBToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
  }
}
