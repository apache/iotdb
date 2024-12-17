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

import org.apache.iotdb.udf.api.type.Type;

import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class TableArgument {
  private final List<String> fieldNames;
  private final List<Type> fieldTypes;
  private final List<String> partitionBy;
  private final List<String> orderBy;

  public TableArgument(
      List<String> fieldNames,
      List<Type> fieldTypes,
      List<String> partitionBy,
      List<String> orderBy) {
    this.fieldNames = requireNonNull(fieldNames, "fieldNames is null");
    this.fieldTypes = requireNonNull(fieldTypes, "fieldTypes is null");
    if (fieldNames.size() != fieldTypes.size()) {
      throw new IllegalArgumentException("fieldNames and fieldTypes must have the same size");
    }
    this.partitionBy = requireNonNull(partitionBy, "partitionBy is null");
    this.orderBy = requireNonNull(orderBy, "orderBy is null");
  }

  public List<String> getFieldNames() {
    return fieldNames;
  }

  public List<Type> getFieldTypes() {
    return fieldTypes;
  }

  public List<String> getPartitionBy() {
    return partitionBy;
  }

  public List<String> getOrderBy() {
    return orderBy;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private List<String> fieldNames = Collections.emptyList();
    private List<Type> fieldTypes = Collections.emptyList();
    private List<String> partitionBy = Collections.emptyList();
    private List<String> orderBy = Collections.emptyList();

    private Builder() {}

    public Builder field(List<String> fieldNames, List<Type> fieldTypes) {
      this.fieldNames = fieldNames;
      this.fieldTypes = fieldTypes;
      return this;
    }

    public Builder partitionBy(List<String> partitionBy) {
      this.partitionBy = partitionBy;
      return this;
    }

    public Builder orderBy(List<String> orderBy) {
      this.orderBy = orderBy;
      return this;
    }

    public TableArgument build() {
      return new TableArgument(fieldNames, fieldTypes, partitionBy, orderBy);
    }
  }
}