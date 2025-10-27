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

package org.apache.iotdb.commons.pipe.datastructure.pattern;

import java.util.Objects;

public abstract class SingleTreePattern extends TreePattern {

  protected final String pattern;

  protected SingleTreePattern(
      final boolean isTreeModelDataAllowedToBeCaptured, final String pattern) {
    super(isTreeModelDataAllowedToBeCaptured);
    this.pattern = pattern != null ? pattern : getDefaultPattern();
  }

  @Override
  public boolean isSingle() {
    return true;
  }

  @Override
  public String getPattern() {
    return pattern;
  }

  @Override
  public boolean isRoot() {
    return Objects.isNull(pattern) || this.pattern.equals(this.getDefaultPattern());
  }

  public abstract String getDefaultPattern();

  @Override
  public String toString() {
    return "{pattern='"
        + pattern
        + "', isTreeModelDataAllowedToBeCaptured="
        + isTreeModelDataAllowedToBeCaptured
        + '}';
  }
}
