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
package org.apache.iotdb.db.queryengine.plan.relational.utils.matching.pattern;

import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Match;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.PatternVisitor;

import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class FilterPattern<T> extends Pattern<T> {
  private final BiPredicate<? super T, ?> predicate;

  public FilterPattern(BiPredicate<? super T, ?> predicate, Optional<Pattern<?>> previous) {
    super(previous);
    this.predicate = requireNonNull(predicate, "predicate is null");
  }

  public BiPredicate<? super T, ?> predicate() {
    return predicate;
  }

  @Override
  public <C> Stream<Match> accept(Object object, Captures captures, C context) {
    // TODO remove cast
    BiPredicate<? super T, C> predicate = (BiPredicate<? super T, C>) this.predicate;
    return Stream.of(Match.of(captures)).filter(match -> predicate.test((T) object, context));
  }

  @Override
  public void accept(PatternVisitor patternVisitor) {
    patternVisitor.visitFilter(this);
  }
}
