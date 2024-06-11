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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateFunction extends Statement {

  private final String udfName;
  private final String className;

  @Nullable private final String uriString;

  public CreateFunction(NodeLocation location, String udfName, String className) {
    super(requireNonNull(location, "location is null"));

    this.udfName = requireNonNull(udfName, "udfName is null");
    this.className = requireNonNull(className, "className is null");
    this.uriString = null;
  }

  public CreateFunction(NodeLocation location, String udfName, String className, String uriString) {
    super(requireNonNull(location, "location is null"));
    this.udfName = requireNonNull(udfName, "udfName is null");
    this.className = requireNonNull(className, "className is null");
    this.uriString = requireNonNull(uriString, "uriString is null");
  }

  public String getUdfName() {
    return udfName;
  }

  public String getClassName() {
    return className;
  }

  public Optional<String> getUriString() {
    return Optional.ofNullable(uriString);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCreateFunction(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateFunction that = (CreateFunction) o;
    return Objects.equals(udfName, that.udfName)
        && Objects.equals(className, that.className)
        && Objects.equals(uriString, that.uriString);
  }

  @Override
  public int hashCode() {
    return Objects.hash(udfName, className, uriString);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("udfName", udfName)
        .add("className", className)
        .add("uriString", uriString)
        .toString();
  }
}
