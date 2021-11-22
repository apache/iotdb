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
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.utils.MetaFormatUtils;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.physical.sys.AppendTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.PruneTemplatePlan;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

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
    if (plan.getSchemaNames() != null) {
      for (String schemaNames : plan.getSchemaNames()) {
        MetaFormatUtils.checkNodeName(schemaNames);
      }
    }

    for (List<String> measurements : plan.getMeasurements()) {
      for (String measurement : measurements) {
        MetaFormatUtils.checkTimeseries(new PartialPath(measurement));
      }
    }

    Template template = new Template(plan);
    if (templateMap.putIfAbsent(plan.getName(), template) != null) {
      // already have template
      throw new MetadataException("Duplicated template name: " + plan.getName());
    }
  }

  public void appendSchemaTemplate(AppendTemplatePlan plan) throws MetadataException {
    String templateName = plan.getName();
    Template temp = templateMap.getOrDefault(templateName, null);
    if (temp != null) {
      String[] measurements = plan.getMeasurements().toArray(new String[0]);
      TSDataType[] dataTypes = new TSDataType[measurements.length];
      TSEncoding[] encodings = new TSEncoding[measurements.length];
      CompressionType[] compressionTypes = new CompressionType[measurements.length];

      for (int i = 0; i < measurements.length; i++) {
        dataTypes[i] = plan.getDataTypes().get(i);
        encodings[i] = plan.getEncodings().get(i);
        compressionTypes[i] = plan.getCompressors().get(i);
      }

      if (plan.isAligned()) {
        temp.addAlignedMeasurements(measurements, dataTypes, encodings, compressionTypes);
      } else {
        temp.addUnalignedMeasurements(measurements, dataTypes, encodings, compressionTypes);
      }
    } else {
      throw new MetadataException("Template does not exists:" + plan.getName());
    }
  }

  public void pruneSchemaTemplate(PruneTemplatePlan plan) throws MetadataException {
    String templateName = plan.getName();
    Template temp = templateMap.getOrDefault(templateName, null);
    if (temp != null) {
      for (int i = 0; i < plan.getPrunedMeasurements().size(); i++) {
        temp.deleteSeriesCascade(plan.getPrunedMeasurements().get(i));
      }
    } else {
      throw new MetadataException("Template does not exists:" + plan.getName());
    }
  }

  public Template getTemplate(String templateName) throws UndefinedTemplateException {
    Template template = templateMap.get(templateName);
    if (template == null) {
      throw new UndefinedTemplateException(templateName);
    }
    return template;
  }

  public void setTemplateMap(Map<String, Template> templateMap) {
    this.templateMap.clear();
    for (Map.Entry<String, Template> templateEntry : templateMap.entrySet()) {
      this.templateMap.put(templateEntry.getKey(), templateEntry.getValue());
    }
  }

  public Map<String, Template> getTemplateMap() {
    return templateMap;
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

    for (String measurementPath : template.getSchemaMap().keySet()) {
      String directNodeName = MetaUtils.splitPathToDetachedPath(measurementPath)[0];
      if (node.hasChild(directNodeName)) {
        throw new MetadataException(
            "Node name "
                + directNodeName
                + " in template has conflict with node's child "
                + (node.getFullPath() + "." + directNodeName));
      }
    }
  }

  public void clear() {
    templateMap.clear();
  }
}
