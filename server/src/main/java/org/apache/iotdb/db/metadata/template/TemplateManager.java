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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.template.DuplicatedTemplateException;
import org.apache.iotdb.db.exception.metadata.template.UndefinedTemplateException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.utils.MetaFormatUtils;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.AppendTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.DropTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.PruneTemplatePlan;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TemplateManager {

  private static final Logger logger = LoggerFactory.getLogger(TemplateManager.class);

  // template name -> template
  private Map<String, Template> templateMap = new ConcurrentHashMap<>();

  // hash the name of template to record it as fixed length in schema file
  private Map<Integer, String> templateHashMap = new ConcurrentHashMap<>();

  private final Map<String, Set<Template>> templateUsageInStorageGroup = new ConcurrentHashMap<>();

  private TemplateLogWriter logWriter;

  private boolean isRecover;

  private static class TemplateManagerHolder {

    private TemplateManagerHolder() {
      // allowed to do nothing
    }

    private static final TemplateManager INSTANCE = new TemplateManager();
  }

  public static TemplateManager getInstance() {
    return TemplateManagerHolder.INSTANCE;
  }

  private TemplateManager() {}

  public void init() throws IOException {
    isRecover = true;
    recoverFromTemplateFile();
    logWriter =
        new TemplateLogWriter(
            IoTDBDescriptor.getInstance().getConfig().getSchemaDir(),
            MetadataConstant.TEMPLATE_FILE);
    isRecover = false;
  }

  private void recoverFromTemplateFile() throws IOException {
    File logFile =
        new File(
            IoTDBDescriptor.getInstance().getConfig().getSchemaDir(),
            MetadataConstant.TEMPLATE_FILE);
    if (!logFile.exists()) {
      return;
    }
    try (TemplateLogReader reader =
        new TemplateLogReader(
            IoTDBDescriptor.getInstance().getConfig().getSchemaDir(),
            MetadataConstant.TEMPLATE_FILE)) {
      PhysicalPlan plan;
      int idx = 0;
      while (reader.hasNext()) {
        try {
          plan = reader.next();
          idx++;
        } catch (Exception e) {
          logger.error("Parse TemplateFile error at lineNumber {} because:", idx, e);
          break;
        }
        if (plan == null) {
          continue;
        }
        try {
          switch (plan.getOperatorType()) {
            case CREATE_TEMPLATE:
              createSchemaTemplate((CreateTemplatePlan) plan);
              break;
            case APPEND_TEMPLATE:
              appendSchemaTemplate((AppendTemplatePlan) plan);
              break;
            case PRUNE_TEMPLATE:
              pruneSchemaTemplate((PruneTemplatePlan) plan);
              break;
            case DROP_TEMPLATE:
              dropSchemaTemplate((DropTemplatePlan) plan);
              break;
            default:
              throw new IOException(
                  "Template file corrupted. Read unknown plan type during recover.");
          }
        } catch (MetadataException | IOException e) {
          logger.error(
              "Can not operate cmd {} in TemplateFile for err:", plan.getOperatorType(), e);
        }
      }
    }
  }

  public void createSchemaTemplate(CreateTemplatePlan plan) throws MetadataException {
    List<List<TSDataType>> dataTypes = plan.getDataTypes();
    List<List<TSEncoding>> encodings = plan.getEncodings();
    for (int i = 0; i < dataTypes.size(); i++) {
      for (int j = 0; j < dataTypes.get(i).size(); j++) {
        SchemaUtils.checkDataTypeWithEncoding(dataTypes.get(i).get(j), encodings.get(i).get(j));
      }
    }

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

    addToHashMap(template);

    try {
      if (!isRecover) {
        logWriter.createSchemaTemplate(plan);
      }
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  /**
   * Calculate and store the unique hash code of all existed template. <br>
   * Collision will be solved by increment now. <br>
   * 0 is the exceptional value for null template.
   *
   * @param template the template to be hashed and added
   */
  private void addToHashMap(Template template) {
    if (templateHashMap.size() >= Integer.MAX_VALUE - 1) {
      logger.error("Too many templates have been registered.");
      return;
    }

    while (templateHashMap.containsKey(template.hashCode())) {
      if (template.hashCode() == Integer.MAX_VALUE) {
        template.setRehash(Integer.MIN_VALUE);
      }

      if (template.hashCode() == 0) {
        template.setRehash(1);
      }

      template.setRehash(template.hashCode() + 1);
    }

    templateHashMap.put(template.hashCode(), template.getName());
  }

  public void dropSchemaTemplate(DropTemplatePlan plan) throws MetadataException {
    templateHashMap.remove(templateMap.get(plan.getName()).hashCode());
    templateMap.remove(plan.getName());

    try {
      if (!isRecover) {
        logWriter.dropSchemaTemplate(plan);
      }
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  public void appendSchemaTemplate(AppendTemplatePlan plan) throws MetadataException {
    List<TSDataType> dataTypeList = plan.getDataTypes();
    List<TSEncoding> encodingList = plan.getEncodings();
    for (int idx = 0; idx < dataTypeList.size(); idx++) {
      SchemaUtils.checkDataTypeWithEncoding(dataTypeList.get(idx), encodingList.get(idx));
    }

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

    try {
      if (!isRecover) {
        logWriter.appendSchemaTemplate(plan);
      }
    } catch (IOException e) {
      throw new MetadataException(e);
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

    try {
      if (!isRecover) {
        logWriter.pruneSchemaTemplate(plan);
      }
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  public Template getTemplate(String templateName) throws UndefinedTemplateException {
    Template template = templateMap.get(templateName);
    if (template == null) {
      throw new UndefinedTemplateException(templateName);
    }
    return template;
  }

  public Template getTemplateFromHash(int hashcode) throws MetadataException {
    if (!templateHashMap.containsKey(hashcode)) {
      throw new MetadataException("Invalid hash code for schema template: " + hashcode);
    }
    return getTemplate(templateHashMap.get(hashcode));
  }

  public void setTemplateMap(Map<String, Template> templateMap) {
    this.templateMap.clear();
    for (Map.Entry<String, Template> templateEntry : templateMap.entrySet()) {
      this.templateMap.put(templateEntry.getKey(), templateEntry.getValue());
      this.addToHashMap(templateEntry.getValue());
    }
  }

  public Map<String, Template> getTemplateMap() {
    return templateMap;
  }

  public Set<String> getAllTemplateName() {
    return templateMap.keySet();
  }

  public void checkIsTemplateCompatible(Template template, IMNode node) throws MetadataException {
    if (node.getSchemaTemplate() != null) {
      if (node.getSchemaTemplate().equals(template)) {
        throw new DuplicatedTemplateException(template.getName());
      } else {
        throw new MetadataException("Specified node already has template");
      }
    }

    // check alignment
    if (node.isEntity() && (node.getAsEntityMNode().isAligned() != template.isDirectAligned())) {
      for (IMNode dNode : template.getDirectNodes()) {
        if (dNode.isMeasurement()) {
          throw new MetadataException(
              String.format(
                  "Template[%s] and mounted node[%s] has different alignment.",
                  template.getName(),
                  node.getFullPath() + IoTDBConstant.PATH_SEPARATOR + dNode.getFullPath()));
        }
      }
    }
  }

  public void markSchemaRegion(
      Template template, String storageGroup, ConsensusGroupId schemaRegionId) {
    synchronized (templateUsageInStorageGroup) {
      if (!templateUsageInStorageGroup.containsKey(storageGroup)) {
        templateUsageInStorageGroup.putIfAbsent(
            storageGroup, Collections.synchronizedSet(new HashSet<>()));
      }
    }
    templateUsageInStorageGroup.get(storageGroup).add(template);
    template.markSchemaRegion(storageGroup, schemaRegionId);
  }

  public void unmarkSchemaRegion(
      Template template, String storageGroup, ConsensusGroupId schemaRegionId) {
    Set<Template> usageInStorageGroup = templateUsageInStorageGroup.get(storageGroup);
    usageInStorageGroup.remove(template);
    synchronized (templateUsageInStorageGroup) {
      if (usageInStorageGroup.isEmpty()) {
        templateUsageInStorageGroup.remove(storageGroup);
      }
    }
    template.unmarkSchemaRegion(storageGroup, schemaRegionId);
  }

  public void forceLog() {
    try {
      logWriter.force();
    } catch (IOException e) {
      logger.error("Cannot force template log", e);
    }
  }

  public void clear() throws IOException {
    templateMap.clear();
    templateUsageInStorageGroup.clear();
    if (logWriter != null) {
      logWriter.close();
    }
  }
}
