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

package org.apache.iotdb.commons.queryengine.plan.relational.type;

import org.apache.iotdb.commons.i18n.QueryMessages;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Represents an opaque identifier for a Type than can be used for serialization or storage in
 * external systems.
 */
public class TypeId {
  private final String id;

  private TypeId(String id) {
    this.id = requireNonNull(id, QueryMessages.EXCEPTION_ID_IS_NULL_9D5D27B1);
  }

  public static TypeId of(String id) {
    return new TypeId(id);
  }

  public String getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TypeId typeId = (TypeId) o;
    return id.equals(typeId.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    return "type:[" + getId() + "]";
  }
}
