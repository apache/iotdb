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

package org.apache.iotdb.db.storageengine.dataregion.modification;

import org.apache.iotdb.commons.path.MeasurementPath;

import java.util.Objects;

/** Modification represents an UPDATE or DELETE operation on a certain timeseries. */
public abstract class Modification {

  protected Type type;
  protected MeasurementPath path;
  protected long fileOffset;

  Modification(Type type, MeasurementPath path, long fileOffset) {
    this.type = type;
    this.path = path;
    this.fileOffset = fileOffset;
  }

  public String getPathString() {
    return path.getFullPath();
  }

  public MeasurementPath getPath() {
    return path;
  }

  public String getDevice() {
    return path.getIDeviceID().toString();
  }

  public String getMeasurement() {
    return path.getMeasurement();
  }

  public void setPath(MeasurementPath path) {
    this.path = path;
  }

  public long getFileOffset() {
    return fileOffset;
  }

  public void setFileOffset(long fileOffset) {
    this.fileOffset = fileOffset;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public enum Type {
    DELETION
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Modification)) {
      return false;
    }
    Modification mod = (Modification) obj;
    return mod.type.equals(this.type)
        && mod.path.equals(this.path)
        && mod.fileOffset == this.fileOffset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, path, fileOffset);
  }
}
