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
package org.apache.iotdb.db.mpp.execution.driver;

import org.apache.iotdb.db.mpp.execution.operator.Operator;

import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.NotThreadSafe;

/** One SchemaDriver is used to execute one FragmentInstance which is for metadata query. */
@NotThreadSafe
public class SchemaDriver extends Driver {

  public SchemaDriver(Operator root, SchemaDriverContext driverContext) {
    super(root, driverContext);
  }

  @Override
  protected boolean init(SettableFuture<?> blockedFuture) {
    return true;
  }

  @Override
  protected void releaseResource() {
    // do nothing
  }
}
