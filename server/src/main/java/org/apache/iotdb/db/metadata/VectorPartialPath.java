/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;

import java.util.List;
import java.util.Objects;

/**
 * VectorPartialPath represents a vector's fullPath. It not only contains the full path of vector's
 * own name, but also has subSensorsPathList which contain all the fullPath of vector's sub sensors.
 * e.g. VectorPartialPath1(root.sg1.d1.vector1, [root.sg1.d1.vector1.s1, root.sg1.d1.vector1.s2])
 * VectorPartialPath2(root.sg1.d1.vector2, [root.sg1.d1.vector2.s1, root.sg1.d1.vector2.s2])
 */
public class VectorPartialPath extends PartialPath {

  private List<PartialPath> subSensorsPathList;

  public VectorPartialPath(String path, List<PartialPath> subSensorsPathList)
      throws IllegalPathException {
    super(path);
    this.subSensorsPathList = subSensorsPathList;
  }

  public List<PartialPath> getSubSensorsPathList() {
    return subSensorsPathList;
  }

  public void setSubSensorsPathList(List<PartialPath> subSensorsPathList) {
    this.subSensorsPathList = subSensorsPathList;
  }

  public void addSubSensor(PartialPath path) {
    this.subSensorsPathList.add(path);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    VectorPartialPath that = (VectorPartialPath) o;
    return Objects.equals(subSensorsPathList, that.subSensorsPathList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), subSensorsPathList);
  }
}
