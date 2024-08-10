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

import org.apache.iotdb.commons.exception.IoTDBException;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.rpc.TSStatusCode.TYPE_NOT_FOUND;

public class TypeNotFoundException extends IoTDBException {

  private final TypeSignature type;

  public TypeNotFoundException(TypeSignature type) {
    this(type, null);
  }

  public TypeNotFoundException(TypeSignature type, Throwable cause) {
    super("Unknown type: " + type, cause, TYPE_NOT_FOUND.getStatusCode());
    this.type = requireNonNull(type, "type is null");
  }

  public TypeSignature getType() {
    return type;
  }
}
