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
import org.apache.iotdb.db.exception.metadata.UndefinedTemplateException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.utils.MetaFormatUtils;
import org.apache.iotdb.db.qp.physical.crud.CreateTemplatePlan;
import org.apache.iotdb.db.utils.TestOnly;

import java.util.List;
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
    // check schema and measurement name before create template
    for (String schemaNames : plan.getSchemaNames()) {
      MetaFormatUtils.checkNodeName(schemaNames);
    }
    for (List<String> measurements : plan.getMeasurements()) {
      MetaFormatUtils.checkSchemaMeasurementNames(measurements);
    }

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

  public void checkIsTemplateAndMNodeCompatible(Template template, IMNode node)
      throws MetadataException {
    if (node.getSchemaTemplate() != null) {
      if (node.getSchemaTemplate().equals(template)) {
        throw new DuplicatedTemplateException(template.getName());
      } else {
        throw new MetadataException("Specified node already has template");
      }
    }

    for (String schemaName : template.getSchemaMap().keySet()) {
      if (node.hasChild(schemaName)) {
        throw new MetadataException(
            "Schema name "
                + schemaName
                + " in template has conflict with node's child "
                + (node.getFullPath() + "." + schemaName));
      }
    }
  }

  public void clear() {
    templateMap.clear();
  }
}
