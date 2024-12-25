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

import java.util.Optional;

/**
 * Abstract class to capture the three supported argument types for a table function: - Table
 * arguments - Descriptor arguments - SQL scalar arguments
 *
 * <p>Each argument is named, and either passed positionally or in a `arg_name => value` convention.
 *
 * <p>Default values are allowed for all arguments except Table arguments.
 */
// TODO:函数注册的时候需要检查 name 不重复
public abstract class ParameterSpecification {
  private final String name;
  private final boolean required;

  // native representation
  private final Optional<Object> defaultValue;

  ParameterSpecification(String name, boolean required, Optional<Object> defaultValue) {
    this.name = name;
    this.required = required;
    this.defaultValue = defaultValue;
    if (required && defaultValue.isPresent()) {
      throw new IllegalArgumentException("non-null default value for a required argument");
    }
  }

  public String getName() {
    return name;
  }

  public boolean isRequired() {
    return required;
  }

  public Optional<Object> getDefaultValue() {
    return defaultValue;
  }
}
