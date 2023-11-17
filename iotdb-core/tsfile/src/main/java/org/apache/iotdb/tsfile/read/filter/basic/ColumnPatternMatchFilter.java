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

package org.apache.iotdb.tsfile.read.filter.basic;

import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/* base class for Like, NotLike, Regex, NotRegex */
public abstract class ColumnPatternMatchFilter implements Filter {

  protected final String regex;
  protected final Pattern pattern;

  protected ColumnPatternMatchFilter(String regex) {
    this.regex = Objects.requireNonNull(regex, "regex cannot be null");
    try {
      this.pattern = Pattern.compile(regex);
    } catch (PatternSyntaxException e) {
      throw new PatternSyntaxException("Regular expression error", regex, e.getIndex());
    }
  }
}
