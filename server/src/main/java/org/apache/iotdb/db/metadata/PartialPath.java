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
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * A prefix path, suffix path or fullPath generated from SQL
 */
public class PartialPath implements Comparable<PartialPath> {

  private String[] nodes;
  private String path;
  private String pathWithoutLastNode;
  private String alias;

  public PartialPath(String path) throws IllegalPathException {
    this.nodes = MetaUtils.splitPathToDetachedPath(path);
    this.path = path;
  }

  public PartialPath(String[] partialNodes) {
    nodes = partialNodes;
  }

  /**
   * it will return a new partial path
   * @param partialPath the path you want to concat
   * @return new partial path
   */
  public PartialPath concatPath(PartialPath partialPath) {
    int len = nodes.length;
    String[] newNodes = Arrays.copyOf(nodes, nodes.length + partialPath.nodes.length);
    System.arraycopy(partialPath.nodes, 0, newNodes, len, partialPath.nodes.length);
    return new PartialPath(newNodes);
  }

  /**
   * It will change nodes in this partial path
   * @param otherNodes nodes
   */
  public void concatPath(String[] otherNodes) {
    int len = nodes.length;
    this.nodes = Arrays.copyOf(nodes, nodes.length + otherNodes.length);
    System.arraycopy(otherNodes, 0, nodes, len, otherNodes.length);
  }

  public PartialPath concatNode(String node) {
    String[] newPathNodes = Arrays.copyOf(nodes, nodes.length + 1);
    newPathNodes[newPathNodes.length - 1] = node;
    return new PartialPath(newPathNodes);
  }

  public String[] getNodes() {
    return nodes;
  }

  @Override
  public PartialPath clone() {
    return new PartialPath(nodes.clone());
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

  @Override
  public boolean equals(Object obj) {
    if(!(obj instanceof PartialPath)) {
      return false;
    }
    String[] otherNodes = ((PartialPath) obj).getNodes();
    if( this.nodes.length != otherNodes.length) {
      return false;
    } else {
      for(int i = 0; i < this.nodes.length; i++) {
        if(!nodes[i].equals(otherNodes[i])) {
          return false;
        }
      }
     }
    return true;
  }

  @Override
  public int hashCode() {
    return this.toString().hashCode();
  }

  public String getLastNode() {
    return nodes[nodes.length - 1];
  }

  public String getFirstNode() {
    return nodes[0];
  }

  public String getPathWithoutLastNode() {
    if (pathWithoutLastNode != null) {
      return pathWithoutLastNode;
    } else {
      StringBuilder s = new StringBuilder(nodes[0]);
      for (int i = 1; i < nodes.length - 1; i++) {
        s.append(TsFileConstant.PATH_SEPARATOR);
        s.append(nodes[i]);
      }
      pathWithoutLastNode = s.toString();
      return pathWithoutLastNode;
    }
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public String getPathWithoutLastNodeWithAlias() {
    return getPathWithoutLastNode() + IoTDBConstant.PATH_SEPARATOR + alias;
  }

  public String getAlias() {
    return alias;
  }

  public boolean startsWith(String[] otherNodes) {
    for(int i = 0; i < otherNodes.length; i++) {
      if(!nodes[i].equals(otherNodes[i])) {
        return false;
      }
    }
    return true;
  }

  public Path toTSFilePath() {
    return new Path(getPathWithoutLastNode(), getLastNode());
  }

  @Override
  public int compareTo(PartialPath o) {
    return this.toString().compareTo(o.toString());
  }
}
