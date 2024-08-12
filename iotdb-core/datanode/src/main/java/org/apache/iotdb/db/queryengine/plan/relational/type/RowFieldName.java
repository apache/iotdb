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

package org.apache.iotdb.db.queryengine.plan.relational.type;

import com.google.errorprone.annotations.Immutable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@Immutable
public final class RowFieldName {
  private final String name;

  public RowFieldName(String name) {
    this.name = requireNonNull(name, "name is null");
  }

  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RowFieldName other = (RowFieldName) o;

    return Objects.equals(this.name, other.name);
  }

  @Override
  public String toString() {
    return '"' + name.replace("\"", "\"\"") + '"';
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
