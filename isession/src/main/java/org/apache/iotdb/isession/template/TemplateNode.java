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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public abstract class TemplateNode {
  private String name;

  public TemplateNode(String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }

  public Map<String, TemplateNode> getChildren() {
    return null;
  }

  public void addChild(TemplateNode node) throws StatementExecutionException {}

  public void deleteChild(TemplateNode node) {}

  public boolean isMeasurement() {
    return false;
  }

  public boolean isShareTime() {
    return false;
  }

  public void serialize(OutputStream buffer) throws IOException {}
}
