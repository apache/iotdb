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
package org.apache.iotdb.isession.template;

import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The template instance constructed within session is SUGGESTED to be a flat measurement template,
 * which has no internal nodes inside a template.
 *
 * <p>For example, template(s1, s2, s3) is a flat measurement template, while template2(GPS.x,
 * GPS.y, s1) is not.
 *
 * <p>Tree-structured template, which is contrary to flat measurement template, may not be supported
 * in further version of IoTDB
 */
public class Template {
  private String name;
  private Map<String, TemplateNode> children;
  private boolean shareTime;

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

  public void addToTemplate(TemplateNode child) throws StatementExecutionException {
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
    Deque<Pair<String, TemplateNode>> stack = new ArrayDeque<>();
    Set<String> alignedPrefix = new HashSet<>();
    ReadWriteIOUtils.write(getName(), baos);
    ReadWriteIOUtils.write(isShareTime(), baos);
    if (isShareTime()) {
      alignedPrefix.add("");
    }

    for (TemplateNode child : children.values()) {
      stack.push(new Pair<>("", child));
    }

    while (stack.size() != 0) {
      Pair<String, TemplateNode> cur = stack.pop();
      String prefix = cur.left;
      TemplateNode curNode = cur.right;
      StringBuilder fullPath = new StringBuilder(prefix);

      if (!curNode.isMeasurement()) {
        if (!"".equals(prefix)) {
          fullPath.append(TsFileConstant.PATH_SEPARATOR);
        }
        fullPath.append(curNode.getName());
        if (curNode.isShareTime()) {
          alignedPrefix.add(fullPath.toString());
        }

        for (TemplateNode child : curNode.getChildren().values()) {
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
