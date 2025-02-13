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

package org.apache.iotdb.udf.api.relational.table.specification;

import org.apache.iotdb.udf.api.type.Type;

import java.util.Optional;

public class ScalarParameterSpecification extends ParameterSpecification {
  private final Type type;

  private ScalarParameterSpecification(
      String name, Type type, boolean required, Object defaultValue) {
    super(name, required, Optional.ofNullable(defaultValue));
    this.type = type;
    if (defaultValue != null && !type.checkObjectType(defaultValue)) {
      throw new IllegalArgumentException(
          String.format(
              "default value %s does not match the declared type: %s", defaultValue, type));
    }
  }

  public Type getType() {
    return type;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String name;
    private Type type;
    private boolean required = true;
    private Object defaultValue;

    private Builder() {}

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder type(Type type) {
      this.type = type;
      return this;
    }

    public Builder defaultValue(Object defaultValue) {
      this.required = false;
      this.defaultValue = defaultValue;
      return this;
    }

    public ScalarParameterSpecification build() {
      return new ScalarParameterSpecification(name, type, required, defaultValue);
    }
  }
}
