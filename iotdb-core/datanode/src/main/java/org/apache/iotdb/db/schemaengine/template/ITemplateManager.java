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

package org.apache.iotdb.db.schemaengine.template;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.CreateSchemaTemplateStatement;

import org.apache.tsfile.utils.Pair;

import java.util.List;
import java.util.Map;

public interface ITemplateManager {

  /**
   * Create device template by sending request to ConfigNode.
   *
   * @param statement CreateSchemaTemplateStatement
   * @return TSStatus
   */
  TSStatus createSchemaTemplate(CreateSchemaTemplateStatement statement);

  /** Show device templates. */
  List<Template> getAllTemplates();

  /**
   * show nodes in device template xx
   *
   * @param name template name
   * @return Template
   */
  Template getTemplate(String name) throws IoTDBException;

  Template getTemplate(int id);

  /**
   * Set template to given path.
   *
   * @param name templateName
   * @param path set path
   */
  void setSchemaTemplate(String queryId, String name, PartialPath path);

  /**
   * Get info of mounted template.
   *
   * @param name template name
   * @param scope scope
   */
  List<PartialPath> getPathsSetTemplate(String name, PathPatternTree scope);

  Pair<Template, PartialPath> checkTemplateSetInfo(PartialPath devicePath);

  Pair<Template, PartialPath> checkTemplateSetAndPreSetInfo(
      PartialPath timeSeriesPath, String alias);

  Pair<Template, List<PartialPath>> getAllPathsSetTemplate(String templateName);

  Map<Integer, Template> checkAllRelatedTemplate(PartialPath pathPattern);

  List<Template> getAllRelatedTemplates(PathPatternTree scope);
}
