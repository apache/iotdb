/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.qp.physical.crud;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.read.common.Path;

public class DeletePlan extends PhysicalPlan {

  private long deleteTime;
  private List<Path> paths = new ArrayList<>();

  public DeletePlan() {
    super(false, Operator.OperatorType.DELETE);
  }

  /**
   * constructor of DeletePlan with single path.
   *
   * @param deleteTime delete time
   * @param path time series path
   */
  public DeletePlan(long deleteTime, Path path) {
    super(false, Operator.OperatorType.DELETE);
    this.deleteTime = deleteTime;
    this.paths.add(path);
  }

  /**
   * constructor of DeletePlan with multiple paths.
   *
   * @param deleteTime delete time
   * @param paths time series paths in List structure
   */
  public DeletePlan(long deleteTime, List<Path> paths) {
    super(false, Operator.OperatorType.DELETE);
    this.deleteTime = deleteTime;
    this.paths = paths;
  }

  public long getDeleteTime() {
    return deleteTime;
  }

  public void setDeleteTime(long delTime) {
    this.deleteTime = delTime;
  }

  public void addPath(Path path) {
    this.paths.add(path);
  }

  @Override
  public List<Path> getPaths() {
    return paths;
  }

  public void setPaths(List<Path> paths) {
    this.paths = paths;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeletePlan that = (DeletePlan) o;
    return deleteTime == that.deleteTime && Objects.equals(paths, that.paths);
  }

}
