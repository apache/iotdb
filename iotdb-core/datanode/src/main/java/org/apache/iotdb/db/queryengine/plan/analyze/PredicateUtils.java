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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.predicate.ConvertExpressionToFilterVisitor;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.predicate.SchemaCompatibilityValidator;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class PredicateUtils {

  private PredicateUtils() {
    // util class
  }

  public static void validateSchemaCompatibility(Expression predicate, PartialPath path) {
    SchemaCompatibilityValidator.validate(predicate, path);
  }

  public static Filter convertPredicateToFilter(Expression predicate, TypeProvider typeProvider) {
    return predicate.accept(new ConvertExpressionToFilterVisitor(), typeProvider);
  }
}
