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
package org.apache.iotdb.udf.api.commons;

/** A simple substitution class for PartialPath in server Module */
public class UDFPartialPath {
  private String[] nodes;

  private String fullPath;

  private static final String PATH_SEPARATOR = ".";

  public UDFPartialPath() {}

  /** @param partialNodes nodes of a time series path */
  public UDFPartialPath(String[] partialNodes) {
    nodes = partialNodes;
  }

  public String[] getNodes() {
    return nodes;
  }

  public String getTailNode() {
    if (nodes.length <= 0) {
      return "";
    }
    return nodes[nodes.length - 1];
  }

  public String getFullPath() {
    if (fullPath == null) {
      StringBuilder s = new StringBuilder(nodes[0]);
      for (int i = 1; i < nodes.length; i++) {
        s.append(PATH_SEPARATOR).append(nodes[i]);
      }
      fullPath = s.toString();
    }
    return fullPath;
  }
}
