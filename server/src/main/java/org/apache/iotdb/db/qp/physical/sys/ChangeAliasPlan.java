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

package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ChangeAliasPlan extends PhysicalPlan {
  private PartialPath path;
  private String alias;

  public ChangeAliasPlan() {
    super(false, Operator.OperatorType.CHANGE_ALIAS);
  }

  public ChangeAliasPlan(PartialPath path, String alias) {
    super(false, Operator.OperatorType.CHANGE_ALIAS);
    this.path = path;
    this.alias = alias;
  }

  public PartialPath getPath() {
    return path;
  }

  public void setPath(PartialPath path) {
    this.path = path;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  @Override
  public List<PartialPath> getPaths() {
    List<PartialPath> ret = new ArrayList<>();
    if (path != null) {
      ret.add(path);
    }
    return ret;
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    int type = PhysicalPlanType.CHANGE_ALIAS.ordinal();
    buffer.put((byte) type);
    putString(buffer, path.getFullPath());
    putString(buffer, alias);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.write((byte) PhysicalPlanType.CHANGE_ALIAS.ordinal());
    putString(stream, path.getFullPath());
    putString(stream, alias);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    path = new PartialPath(readString(buffer));
    alias = readString(buffer);
  }

  @Override
  public String toString() {
    return "ChangeAlias{" + path + "," + alias + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ChangeAliasPlan that = (ChangeAliasPlan) o;
    return Objects.equals(path, that.path) && Objects.equals(alias, that.alias);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, alias);
  }
}
