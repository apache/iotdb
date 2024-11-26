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

package org.apache.tsfile.read.common.type;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.tsfile.read.common.type.TypeEnum.ROW;

public class RowType extends AbstractType {

  private final List<Field> fields;
  private final List<Type> fieldTypes;
  private final boolean comparable;
  private final boolean orderable;

  private RowType(List<Field> originalFields) {

    this.fields = new ArrayList<>(originalFields);
    this.fieldTypes = fields.stream().map(Field::getType).collect(Collectors.toList());

    this.comparable = fields.stream().allMatch(field -> field.getType().isComparable());
    this.orderable = fields.stream().allMatch(field -> field.getType().isOrderable());
  }

  public static RowType from(List<Field> fields) {
    return new RowType(fields);
  }

  public static RowType anonymous(List<Type> types) {
    List<Field> fields =
        types.stream().map(type -> new Field(Optional.empty(), type)).collect(Collectors.toList());

    return new RowType(fields);
  }

  public static RowType rowType(Field... field) {
    return from(Arrays.asList(field));
  }

  public static RowType anonymousRow(Type... types) {
    return anonymous(Arrays.asList(types));
  }

  // Only RowParametricType.createType should call this method
  public static RowType createWithTypeSignature(List<Field> fields) {
    return new RowType(fields);
  }

  public static Field field(String name, Type type) {
    return new Field(Optional.of(name), type);
  }

  public static Field field(Type type) {
    return new Field(Optional.empty(), type);
  }

  @Override
  public ColumnBuilder createColumnBuilder(int expectedEntries) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeEnum getTypeEnum() {
    return ROW;
  }

  @Override
  public String getDisplayName() {
    // Convert to standard sql name
    StringBuilder result = new StringBuilder();
    result.append("ROW").append('(');
    for (Field field : fields) {
      String typeDisplayName = field.getType().getDisplayName();
      if (field.getName().isPresent()) {
        // TODO: names are already canonicalized, so they should be printed as delimited identifiers
        result.append(field.getName().get()).append(' ').append(typeDisplayName);
      } else {
        result.append(typeDisplayName);
      }
      result.append(", ");
    }
    result.setLength(result.length() - 2);
    result.append(')');
    return result.toString();
  }

  @Override
  public List<Type> getTypeParameters() {
    return fieldTypes;
  }

  public List<Field> getFields() {
    return fields;
  }

  public static class Field {
    private final Type type;
    private final Optional<String> name;

    public Field(Optional<String> name, Type type) {
      this.type = requireNonNull(type, "type is null");
      this.name = requireNonNull(name, "name is null");
    }

    public Type getType() {
      return type;
    }

    public Optional<String> getName() {
      return name;
    }
  }

  @Override
  public boolean isComparable() {
    return comparable;
  }

  @Override
  public boolean isOrderable() {
    return orderable;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RowType rowType = (RowType) o;
    return comparable == rowType.comparable
        && orderable == rowType.orderable
        && Objects.equals(fields, rowType.fields)
        && Objects.equals(fieldTypes, rowType.fieldTypes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fields, fieldTypes, comparable, orderable);
  }
}
