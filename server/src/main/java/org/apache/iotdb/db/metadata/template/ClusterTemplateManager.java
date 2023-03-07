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
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.rpc.thrift.TCreateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaTemplateReq;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeClientManager;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

public class ClusterTemplateManager implements ITemplateManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTemplateManager.class);

  // <TemplateId, Template>
  private final Map<Integer, Template> templateIdMap = new ConcurrentHashMap<>();
  // <TemplateName, TemplateId>
  private final Map<String, Integer> templateNameMap = new ConcurrentHashMap<>();
  // <FullPath, TemplateId>
  private final Map<PartialPath, Integer> pathSetTemplateMap = new ConcurrentHashMap<>();
  // <TemplateId, List<FullPath>>
  private final Map<Integer, List<PartialPath>> templateSetOnPathsMap = new ConcurrentHashMap<>();

  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  private static final class ClusterTemplateManagerHolder {
    private static final ClusterTemplateManager INSTANCE = new ClusterTemplateManager();

    private ClusterTemplateManagerHolder() {}
  }

  public static ClusterTemplateManager getInstance() {
    return ClusterTemplateManager.ClusterTemplateManagerHolder.INSTANCE;
  }

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  @Override
  public TSStatus createSchemaTemplate(CreateSchemaTemplateStatement statement) {
    TCreateSchemaTemplateReq req = constructTCreateSchemaTemplateReq(statement);
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      // Send request to some API server
      TSStatus tsStatus = configNodeClient.createSchemaTemplate(req);
      // Get response or throw exception
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.error(
            "Failed to execute create schema template {} in config node, status is {}.",
            statement.getName(),
            tsStatus);
      }
      return tsStatus;
    } catch (ClientManagerException | TException e) {
      throw new RuntimeException(
          new IoTDBException(
              "create template error.", e, TSStatusCode.CREATE_TEMPLATE_ERROR.getStatusCode()));
    }
  }

  private TCreateSchemaTemplateReq constructTCreateSchemaTemplateReq(
      CreateSchemaTemplateStatement statement) {
    TCreateSchemaTemplateReq req = new TCreateSchemaTemplateReq();
    try {
      Template template =
          new Template(
              statement.getName(),
              statement.getMeasurements(),
              statement.getDataTypes(),
              statement.getEncodings(),
              statement.getCompressors(),
              statement.isAligned());
      req.setName(template.getName());
      req.setSerializedTemplate(template.serialize());
    } catch (IllegalPathException e) {
      throw new RuntimeException(e);
    }
    return req;
  }

  @Override
  public List<Template> getAllTemplates() {
    List<Template> templatesList = new ArrayList<>();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TGetAllTemplatesResp tGetAllTemplatesResp = configNodeClient.getAllTemplates();
      // Get response or throw exception
      if (tGetAllTemplatesResp.getStatus().getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        List<ByteBuffer> list = tGetAllTemplatesResp.getTemplateList();
        list.forEach(
            templateData -> {
              Template template = new Template();
              template.deserialize(templateData);
              templatesList.add(template);
            });
      } else {
        throw new RuntimeException(
            new IoTDBException(
                tGetAllTemplatesResp.getStatus().getMessage(),
                tGetAllTemplatesResp.getStatus().getCode()));
      }
    } catch (ClientManagerException | TException e) {
      throw new RuntimeException(
          new IoTDBException(
              "get all template error.", TSStatusCode.UNDEFINED_TEMPLATE.getStatusCode()));
    }
    return templatesList;
  }

  @Override
  public Template getTemplate(String name) {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TGetTemplateResp resp = configNodeClient.getTemplate(name);
      if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        byte[] templateBytes = resp.getTemplate();
        Template template = new Template();
        template.deserialize(ByteBuffer.wrap(templateBytes));
        return template;
      } else {
        throw new RuntimeException(
            new IoTDBException(resp.status.getMessage(), resp.status.getCode()));
      }
    } catch (Exception e) {
      throw new RuntimeException(
          new IoTDBException(
              "get template info error.", TSStatusCode.UNDEFINED_TEMPLATE.getStatusCode()));
    }
  }

  @Override
  public void setSchemaTemplate(String name, PartialPath path) {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TSetSchemaTemplateReq req = new TSetSchemaTemplateReq();
      req.setName(name);
      req.setPath(path.getFullPath());
      TSStatus tsStatus = configNodeClient.setSchemaTemplate(req);
      if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new IoTDBException(tsStatus.getMessage(), tsStatus.getCode());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<PartialPath> getPathsSetTemplate(String name) {
    List<PartialPath> listPath = new ArrayList<>();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TGetPathsSetTemplatesResp resp = configNodeClient.getPathsSetTemplate(name);
      if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        if (resp.getPathList() != null) {
          resp.getPathList()
              .forEach(
                  item -> {
                    try {
                      listPath.add(new PartialPath(item));
                    } catch (IllegalPathException e) {
                      e.printStackTrace();
                    }
                  });
        }
      } else {
        throw new IoTDBException(resp.status.getMessage(), resp.status.getCode());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return listPath;
  }

  @Override
  public Template getTemplate(int id) {
    readWriteLock.readLock().lock();
    try {
      return templateIdMap.get(id);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public Pair<Template, PartialPath> checkTemplateSetInfo(PartialPath path) {
    readWriteLock.readLock().lock();
    try {
      for (PartialPath templateSetPath : pathSetTemplateMap.keySet()) {
        if (path.startsWith(templateSetPath.getNodes())) {
          return new Pair<>(
              templateIdMap.get(pathSetTemplateMap.get(templateSetPath)), templateSetPath);
        }
      }
      return null;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public Pair<Template, List<PartialPath>> getAllPathsSetTemplate(String templateName) {
    readWriteLock.readLock().lock();
    try {
      if (!templateNameMap.containsKey(templateName)) {
        return null;
      }
      Template template = templateIdMap.get(templateNameMap.get(templateName));
      return new Pair<>(template, templateSetOnPathsMap.get(template.getId()));
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public Map<Integer, Template> checkAllRelatedTemplate(PartialPath pathPattern) {
    readWriteLock.readLock().lock();
    try {
      Map<Integer, Template> result = new HashMap<>();
      int templateId;
      Template template;
      for (Map.Entry<PartialPath, Integer> entry : pathSetTemplateMap.entrySet()) {
        templateId = entry.getValue();
        template = templateIdMap.get(templateId);
        if (checkIsRelated(pathPattern, entry.getKey(), template)) {
          result.put(templateId, template);
        }
      }
      return result;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  private boolean checkIsRelated(
      PartialPath pathPattern, PartialPath pathSetTemplate, Template template) {
    // e.g. given template t1(s1 int32) set on root.sg, the possible timeseries are matched by
    // root.sg.s1 or root.sg.**.s1
    // thus we check pathPattern.overlapWith(root.sg.s1) || pathPattern.overlapWith(root.sg.**.s1)
    // and the following logic is equivalent with the above expression

    String measurement = pathPattern.getTailNode();
    if (measurement.equals(MULTI_LEVEL_PATH_WILDCARD)
        || measurement.equals(ONE_LEVEL_PATH_WILDCARD)) {
      return pathPattern.overlapWith(
              pathSetTemplate
                  .concatNode(MULTI_LEVEL_PATH_WILDCARD)
                  .concatNode(ONE_LEVEL_PATH_WILDCARD))
          || pathPattern.overlapWith(pathSetTemplate.concatNode(ONE_LEVEL_PATH_WILDCARD));
    }

    if (template.hasSchema(measurement)) {
      return pathPattern.overlapWith(
              pathSetTemplate.concatNode(MULTI_LEVEL_PATH_WILDCARD).concatNode(measurement))
          || pathPattern.overlapWith(pathSetTemplate.concatNode(measurement));
    }

    return false;
  }

  public void updateTemplateSetInfo(byte[] templateSetInfo) {
    if (templateSetInfo == null) {
      return;
    }
    readWriteLock.writeLock().lock();
    try {
      ByteBuffer buffer = ByteBuffer.wrap(templateSetInfo);

      Map<Template, List<String>> parsedTemplateSetInfo =
          TemplateInternalRPCUtil.parseAddTemplateSetInfoBytes(buffer);
      for (Map.Entry<Template, List<String>> entry : parsedTemplateSetInfo.entrySet()) {
        Template template = entry.getKey();
        templateIdMap.put(template.getId(), template);
        templateNameMap.put(template.getName(), template.getId());

        for (String pathSetTemplate : entry.getValue()) {
          try {
            PartialPath path = new PartialPath(pathSetTemplate);
            pathSetTemplateMap.put(path, template.getId());
            List<PartialPath> pathList =
                templateSetOnPathsMap.computeIfAbsent(
                    template.getId(), integer -> new ArrayList<>());
            pathList.add(path);
          } catch (IllegalPathException ignored) {

          }
        }
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void invalidateTemplateSetInfo(byte[] templateSetInfo) {
    if (templateSetInfo == null) {
      return;
    }
    readWriteLock.writeLock().lock();
    try {
      ByteBuffer buffer = ByteBuffer.wrap(templateSetInfo);
      Pair<Integer, String> parsedInfo =
          TemplateInternalRPCUtil.parseInvalidateTemplateSetInfoBytes(buffer);
      int templateId = parsedInfo.left;
      String pathSetTemplate = parsedInfo.right;
      try {
        PartialPath path = new PartialPath(pathSetTemplate);
        pathSetTemplateMap.remove(path);
        if (templateSetOnPathsMap.containsKey(templateId)) {
          templateSetOnPathsMap.get(templateId).remove(path);
          if (templateSetOnPathsMap.get(templateId).isEmpty()) {
            templateSetOnPathsMap.remove(templateId);
            Template template = templateIdMap.remove(templateId);
            templateNameMap.remove(template.getName());
          }
        }
      } catch (IllegalPathException ignored) {

      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }
}
