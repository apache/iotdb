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

package org.apache.iotdb.db.mpp.common.object;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public abstract class ObjectEntry {

  private int id = -1;

  protected ObjectEntry() {}

  public int getId() {
    return id;
  }

  final void setId(int id) {
    this.id = id;
  }

  public final boolean isRegistered() {
    return id >= 0;
  }

  public abstract ObjectType getType();

  public final void serializeObject(DataOutputStream dataOutputStream) throws IOException {
    getType().serialize(dataOutputStream);
    serializeObjectData(dataOutputStream);
  }

  public final void deserializeObject(DataInputStream dataInputStream) throws IOException {
    deserializeObjectData(dataInputStream);
  }

  protected abstract void serializeObjectData(DataOutputStream dataOutputStream) throws IOException;

  protected abstract void deserializeObjectData(DataInputStream dataInputStream) throws IOException;
}
