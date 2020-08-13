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

import java.util.Arrays;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

/**
 * A prefix path, suffix path or fullPath generated from SQL
 */
public class PartialPath {

  private String[] nodes;
  private String path;


  public PartialPath(String[] partialNodes) {
    nodes = partialNodes;
  }

  public void concatPath(PartialPath partialPath) {
    int len = nodes.length;
    this.nodes = Arrays.copyOf(nodes, nodes.length + partialPath.nodes.length);
    System.arraycopy(partialPath.nodes, 0, nodes, len, partialPath.nodes.length);
  }

  public String[] getNodes() {
    return nodes;
  }

  @Override
  public PartialPath clone() {
    return new PartialPath(nodes);
  }

  public String toString() {
    if (path != null) {
      return path;
    } else {
      StringBuilder s = new StringBuilder(nodes[0]);
      for (int i = 1; i < nodes.length; i++) {
        s.append(TsFileConstant.PATH_SEPARATOR);
        s.append(nodes[i]);
      }
      path = s.toString();
      return path;
    }
  }
}
