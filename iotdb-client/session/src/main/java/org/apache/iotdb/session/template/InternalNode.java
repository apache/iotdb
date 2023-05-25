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

import org.apache.iotdb.isession.template.TemplateNode;
import org.apache.iotdb.rpc.StatementExecutionException;

import java.util.HashMap;
import java.util.Map;

public class InternalNode extends TemplateNode {
  private Map<String, TemplateNode> children;
  private boolean shareTime;

  public InternalNode(String name, boolean shareTime) {
    super(name);
    this.children = new HashMap<>();
    this.shareTime = shareTime;
  }

  @Override
  public void addChild(TemplateNode node) throws StatementExecutionException {
    if (children.containsKey(node.getName())) {
      throw new StatementExecutionException("Duplicated child of node in template.");
    }
    this.children.put(node.getName(), node);
  }

  @Override
  public void deleteChild(TemplateNode node) {
    if (children.containsKey(node.getName())) {
      children.remove(node.getName());
    }
  }

  public Map<String, TemplateNode> getChildren() {
    return children;
  }

  @Override
  public boolean isShareTime() {
    return shareTime;
  }
}
