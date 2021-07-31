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

import org.apache.iotdb.db.exception.metadata.DuplicatedTemplateException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.UndefinedTemplateException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.qp.physical.crud.CreateTemplatePlan;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TemplateManager {

  // template name -> template
  private Map<String, Template> templateMap = new ConcurrentHashMap<>();

  private static class TemplateManagerHolder {

    private TemplateManagerHolder() {
      // allowed to do nothing
    }

    private static final TemplateManager INSTANCE = new TemplateManager();
  }

  public static TemplateManager getInstance() {
    return TemplateManagerHolder.INSTANCE;
  }

  @TestOnly
  public static TemplateManager getNewInstanceForTest() {
    return new TemplateManager();
  }

  private TemplateManager() {}

  public void createSchemaTemplate(CreateTemplatePlan plan) throws MetadataException {
    Template template = new Template(plan);
    if (templateMap.putIfAbsent(plan.getName(), template) != null) {
      // already have template
      throw new MetadataException("Duplicated template name: " + plan.getName());
    }
  }

  public Template getTemplate(String templateName) throws UndefinedTemplateException {
    Template template = templateMap.get(templateName);
    if (template == null) {
      throw new UndefinedTemplateException(templateName);
    }
    return template;
  }

  public void setSchemaTemplate(Template template, Pair<IMNode, Template> node)
      throws MetadataException {

    if (node.left.getSchemaTemplate() != null) {
      if (node.left.getSchemaTemplate().equals(template)) {
        throw new DuplicatedTemplateException(template.getName());
      } else {
        throw new MetadataException("Specified node already has template");
      }
    }

    if (!isTemplateCompatible(node.right, template)) {
      throw new MetadataException("Incompatible template");
    }

    checkIsTemplateAndMNodeCompatible(template, node.left);

    node.left.setSchemaTemplate(template);
  }

  public boolean isTemplateCompatible(Template upper, Template current) {
    if (upper == null) {
      return true;
    }

    Map<String, IMeasurementSchema> upperMap = new HashMap<>(upper.getSchemaMap());
    Map<String, IMeasurementSchema> currentMap = new HashMap<>(current.getSchemaMap());

    // for identical vector schema, we should just compare once
    Map<IMeasurementSchema, IMeasurementSchema> sameSchema = new HashMap<>();

    for (String name : currentMap.keySet()) {
      IMeasurementSchema upperSchema = upperMap.remove(name);
      if (upperSchema != null) {
        IMeasurementSchema currentSchema = currentMap.get(name);
        // use "==" to compare actual address space
        if (upperSchema == sameSchema.get(currentSchema)) {
          continue;
        }

        if (!upperSchema.equals(currentSchema)) {
          return false;
        }

        sameSchema.put(currentSchema, upperSchema);
      }
    }

    // current template must contains all measurements of upper template
    return upperMap.isEmpty();
  }

  public void checkIsTemplateAndMNodeCompatible(Template template, IMNode IMNode)
      throws PathAlreadyExistException {
    for (String schemaName : template.getSchemaMap().keySet()) {
      if (IMNode.hasChild(schemaName)) {
        throw new PathAlreadyExistException(
            IMNode.getPartialPath().concatNode(schemaName).getFullPath());
      }
    }
  }

  public void clear() {
    templateMap.clear();
  }
}
