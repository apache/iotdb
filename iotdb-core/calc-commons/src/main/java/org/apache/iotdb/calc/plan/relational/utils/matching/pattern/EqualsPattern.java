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
package org.apache.iotdb.calc.plan.relational.utils.matching.pattern;

import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.plan.relational.utils.matching.Captures;
import org.apache.iotdb.calc.plan.relational.utils.matching.Match;
import org.apache.iotdb.calc.plan.relational.utils.matching.Pattern;
import org.apache.iotdb.calc.plan.relational.utils.matching.PatternVisitor;

import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class EqualsPattern<T> extends Pattern<T> {
  private final T expectedValue;

  public EqualsPattern(T expectedValue, Optional<Pattern<?>> previous) {
    super(previous);
    this.expectedValue =
        requireNonNull(
            expectedValue,
            CalcMessages
                .EXCEPTION_EXPECTEDVALUE_CAN_QUOTE_T_BE_NULL_DOT_USE_ISNULL_LEFT_PAREN_RIGHT_PAREN_PATTERN__FC25E374);
  }

  public T expectedValue() {
    return expectedValue;
  }

  @Override
  public <C> Stream<Match> accept(Object object, Captures captures, C context) {
    return Stream.of(Match.of(captures)).filter(match -> expectedValue.equals(object));
  }

  @Override
  public void accept(PatternVisitor patternVisitor) {
    patternVisitor.visitEquals(this);
  }
}
