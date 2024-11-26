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

package org.apache.tsfile.common.regexp;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LikePattern {
  private final String pattern;
  private final Optional<Character> escape;
  private final LikeMatcher matcher;

  public static LikePattern compile(String pattern, Optional<Character> escape) {
    return new LikePattern(pattern, escape, LikeMatcher.compile(pattern, escape));
  }

  public static LikePattern compile(String pattern, Optional<Character> escape, boolean optimize) {
    return new LikePattern(pattern, escape, LikeMatcher.compile(pattern, escape, optimize));
  }

  private LikePattern(String pattern, Optional<Character> escape, LikeMatcher matcher) {
    this.pattern = requireNonNull(pattern, "pattern is null");
    this.escape = requireNonNull(escape, "escape is null");
    this.matcher = requireNonNull(matcher, "likeMatcher is null");
  }

  public String getPattern() {
    return pattern;
  }

  public Optional<Character> getEscape() {
    return escape;
  }

  public LikeMatcher getMatcher() {
    return matcher;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LikePattern that = (LikePattern) o;
    return Objects.equals(pattern, that.pattern) && Objects.equals(escape, that.escape);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pattern, escape);
  }

  @Override
  public String toString() {
    return "LikePattern{"
        + "pattern='"
        + pattern
        + '\''
        + (escape.map(character -> ", escape=" + character).orElse(""))
        + '}';
  }
}
