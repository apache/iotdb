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
 *
 */
package org.apache.iotdb.db.sync.pipedata;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.sync.receiver.load.DeletionLoader;
import org.apache.iotdb.db.sync.receiver.load.ILoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

public class DeletionPipeData extends PipeData {
  private static final Logger logger = LoggerFactory.getLogger(DeletionPipeData.class);

  private Deletion deletion;

  public DeletionPipeData(Deletion deletion, long serialNumber) {
    super(serialNumber);
    this.deletion = deletion;
  }

  @Override
  public PipeDataType getType() {
    return PipeDataType.DELETION;
  }

  @Override
  public long serialize(DataOutputStream stream) throws IOException {
    return super.serialize(stream) + deletion.serializeWithoutFileOffset(stream);
  }

  public static DeletionPipeData deserialize(DataInputStream stream)
      throws IOException, IllegalPathException {
    long serialNumber = stream.readLong();
    Deletion deletion = Deletion.deserializeWithoutFileOffset(stream);
    return new DeletionPipeData(deletion, serialNumber);
  }

  @Override
  public ILoader createLoader() {
    return new DeletionLoader(deletion);
  }

  @Override
  public String toString() {
    return "DeletionData{" + "serialNumber=" + serialNumber + ", deletion=" + deletion + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DeletionPipeData that = (DeletionPipeData) o;
    return Objects.equals(deletion, that.deletion)
        && Objects.equals(serialNumber, that.serialNumber);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deletion, serialNumber);
  }
}
