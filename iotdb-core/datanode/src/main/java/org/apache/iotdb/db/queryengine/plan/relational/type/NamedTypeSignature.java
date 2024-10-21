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
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Immutable
public final class NamedTypeSignature {
  private final Optional<RowFieldName> fieldName;
  private final TypeSignature typeSignature;

  public NamedTypeSignature(Optional<RowFieldName> fieldName, TypeSignature typeSignature) {
    this.fieldName = requireNonNull(fieldName, "fieldName is null");
    this.typeSignature = requireNonNull(typeSignature, "typeSignature is null");
  }

  public Optional<RowFieldName> getFieldName() {
    return fieldName;
  }

  public TypeSignature getTypeSignature() {
    return typeSignature;
  }

  public Optional<String> getName() {
    return getFieldName().map(RowFieldName::getName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    NamedTypeSignature other = (NamedTypeSignature) o;

    return Objects.equals(this.fieldName, other.fieldName)
        && Objects.equals(this.typeSignature, other.typeSignature);
  }

  @Override
  public String toString() {
    if (fieldName.isPresent()) {
      return format("\"%s\" %s", fieldName.get().getName().replace("\"", "\"\""), typeSignature);
    }
    return typeSignature.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, typeSignature);
  }
}
