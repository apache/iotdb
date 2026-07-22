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

package org.apache.iotdb.commons.queryengine.plan.relational.function;

import org.apache.iotdb.commons.i18n.QueryMessages;

import java.util.Locale;

import static java.util.Objects.requireNonNull;

public class FunctionId {
  public static final FunctionId NOOP_FUNCTION_ID = new FunctionId("noop");

  private final String id;

  public FunctionId(String id) {
    requireNonNull(id, QueryMessages.EXCEPTION_ID_IS_NULL_9D5D27B1);
    if (id.isEmpty()) {
      throw new IllegalArgumentException(QueryMessages.FUNCTION_ID_MUST_NOT_BE_EMPTY);
    }
    if (!id.toLowerCase(Locale.US).equals(id)) {
      throw new IllegalArgumentException(QueryMessages.FUNCTION_ID_MUST_BE_LOWERCASE);
    }
    if (id.contains("@")) {
      throw new IllegalArgumentException(QueryMessages.FUNCTION_ID_MUST_NOT_CONTAIN_AT);
    }
    this.id = id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FunctionId that = (FunctionId) o;
    return id.equals(that.id);
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public String toString() {
    return id;
  }

  public static FunctionId toFunctionId(String canonicalName, Signature signature) {
    return new FunctionId((canonicalName + signature).toLowerCase(Locale.US));
  }
}
