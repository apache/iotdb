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
package org.apache.iotdb.db.queryengine.plan.relational.utils.matching;

import static java.util.Objects.requireNonNull;

public class PropertyPattern<F, C, R> {
  private final Property<F, C, ?> property;
  private final Pattern<R> pattern;

  public static <F, C, T, R> PropertyPattern<F, C, R> of(
      Property<F, C, T> property, Pattern<R> pattern) {
    return new PropertyPattern<>(property, pattern);
  }

  private PropertyPattern(Property<F, C, ?> property, Pattern<R> pattern) {
    this.property = requireNonNull(property, "property is null");
    this.pattern = requireNonNull(pattern, "pattern is null");
  }

  public Property<F, C, ?> getProperty() {
    return property;
  }

  public Pattern<R> getPattern() {
    return pattern;
  }

  // This expresses the fact that PropertyPattern<F, C, T> is covariant on T.
  @SuppressWarnings("unchecked cast")
  public static <F, C, T> PropertyPattern<F, C, T> upcast(
      PropertyPattern<F, C, ? extends T> propertyPattern) {
    return (PropertyPattern<F, C, T>) propertyPattern;
  }
}
