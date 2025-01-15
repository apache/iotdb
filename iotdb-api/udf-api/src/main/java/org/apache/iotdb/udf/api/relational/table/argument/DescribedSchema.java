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

package org.apache.iotdb.udf.api.relational.table.argument;

import org.apache.iotdb.udf.api.type.ColumnCategory;
import org.apache.iotdb.udf.api.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DescribedSchema {
  private final List<Field> fields;

  private DescribedSchema(List<Field> fields) {
    requireNonNull(fields, "fields is null");
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("DescribedSchema has no fields");
    }
    this.fields = fields;
  }

  public List<Field> getFields() {
    return fields;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final List<Field> fields = new ArrayList<>();

    public Builder addField(String name, Type type, ColumnCategory category) {
      fields.add(new Field(name, type, category));
      return this;
    }

    public Builder addField(Optional<String> name, Type type, ColumnCategory category) {
      fields.add(new Field(name, type, category));
      return this;
    }

    public DescribedSchema build() {
      return new DescribedSchema(fields);
    }
  }

  public static class Field {
    private final Optional<String> name;
    private final Type type;
    private final ColumnCategory category;

    public Field(String name, Type type, ColumnCategory category) {
      this.name = Optional.ofNullable(name);
      this.type = type;
      this.category = category;
    }

    public Field(Optional<String> name, Type type, ColumnCategory category) {
      this.name = name;
      this.type = type;
      this.category = category;
    }

    public Optional<String> getName() {
      return name;
    }

    public Type getType() {
      return type;
    }

    public ColumnCategory getCategory() {
      return category;
    }
  }
}
