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

package org.apache.iotdb.db.metadata.template;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;
import java.util.Map;

public interface ITemplateManager {

  /**
   * @param statement CreateSchemaTemplateStatement
   * @return TSStatus
   */
  TSStatus createSchemaTemplate(CreateSchemaTemplateStatement statement);

  /**
   * show schema templates
   *
   * @return List<Template>
   */
  List<Template> getAllTemplates();

  /**
   * show nodes in schema template xx
   *
   * @param name
   * @return Template
   */
  Template getTemplate(String name);

  Template getTemplate(int id);

  /**
   * mount template
   *
   * @param name templateName
   * @param path mount path
   */
  void setSchemaTemplate(String name, PartialPath path);

  /**
   * get info of mounted template
   *
   * @param name
   * @return
   */
  List<PartialPath> getPathsSetTemplate(String name);

  Pair<Template, PartialPath> checkTemplateSetInfo(PartialPath path);

  Pair<Template, List<PartialPath>> getAllPathsSetTemplate(String templateName);

  Map<Integer, Template> checkAllRelatedTemplate(PartialPath pathPattern);
}
