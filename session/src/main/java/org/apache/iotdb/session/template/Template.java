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
package org.apache.iotdb.session.template;

import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javafx.util.Pair;

public class Template {
  private String name;
  private Map<String, Node> children;
  private boolean shareTime;

  // sync with metadata.Template
  public enum TemplateQueryType {
    NULL,
    COUNT_MEASUREMENTS,
    IS_MEASUREMENT,
    IS_SERIES,
    SHOW_MEASUREMENTS
  }

  public Template(String name, boolean isShareTime) {
    this.name = name;
    this.children = new HashMap<>();
    this.shareTime = isShareTime;
  }

  public Template(String name) {
    this(name, false);
  }

  public String getName() {
    return name;
  }

  public boolean isShareTime() {
    return shareTime;
  }

  public void setShareTime(boolean shareTime) {
    this.shareTime = shareTime;
  }

  // region Interface to manipulate Template

  public void addToTemplate(Node child) throws StatementExecutionException {
    if (children.containsKey(child.getName())) {
      throw new StatementExecutionException("Duplicated child of node in template.");
    }
    children.put(child.getName(), child);
  }

  public void deleteFromTemplate(String name) throws StatementExecutionException {
    if (children.containsKey(name)) {
      children.remove(name);
    } else {
      throw new StatementExecutionException("It is not a direct child of the template: " + name);
    }
  }

  /** Serialize: templateName[string] isShareTime[boolean] */
  public void serialize(OutputStream baos) throws IOException {
    Deque<Pair<String, Node>> stack = new ArrayDeque<>();
    Set<String> alignedPrefix = new HashSet<>();
    ReadWriteIOUtils.write(getName(), baos);
    ReadWriteIOUtils.write(isShareTime(), baos);

    for (Node child : children.values()) {
      stack.push(new Pair<>("", child));
    }

    while (stack.size() != 0) {
      Pair<String, Node> cur = stack.pop();
      String prefix = cur.getKey();
      Node curNode = cur.getValue();
      StringBuilder fullPath = new StringBuilder(prefix);

      if (!curNode.isMeasurement()) {
        if (!prefix.equals("")) {
          fullPath.append(TsFileConstant.PATH_SEPARATOR);
        }
        fullPath.append(curNode.getName());
        if (curNode.isShareTime()) {
          alignedPrefix.add(fullPath.toString());
        }

        for (Node child : curNode.getChildren().values()) {
          stack.push(new Pair<>(fullPath.toString(), child));
        }
      } else {
        // For each measurement, serialized as: prefixPath, isAlgined, [MeasurementNode]
        ReadWriteIOUtils.write(prefix, baos);
        if (alignedPrefix.contains(prefix)) {
          ReadWriteIOUtils.write(true, baos);
        } else {
          ReadWriteIOUtils.write(false, baos);
        }
        curNode.serialize(baos);
      }
    }
  }

  // endregion

}
