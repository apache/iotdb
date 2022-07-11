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
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.rpc.thrift.TCreateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaTemplateReq;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.client.DataNodeClientPoolFactory;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ClusterTemplateManager implements ITemplateManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTemplateManager.class);

  private static final class ClusterTemplateManagerHolder {
    private static final ClusterTemplateManager INSTANCE = new ClusterTemplateManager();

    private ClusterTemplateManagerHolder() {}
  }

  public static ClusterTemplateManager getInstance() {
    return ClusterTemplateManager.ClusterTemplateManagerHolder.INSTANCE;
  }

  private static final IClientManager<PartitionRegionId, ConfigNodeClient>
      CONFIG_NODE_CLIENT_MANAGER =
          new IClientManager.Factory<PartitionRegionId, ConfigNodeClient>()
              .createClientManager(new DataNodeClientPoolFactory.ConfigNodeClientPoolFactory());

  @Override
  public TSStatus createSchemaTemplate(CreateSchemaTemplateStatement statement) {
    TCreateSchemaTemplateReq req = constructTCreateSchemaTemplateReq(statement);
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
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
    } catch (TException | IOException e) {
      throw new RuntimeException(
          new IoTDBException(
              "create template error.", e, TSStatusCode.CREATE_TEMPLATE_ERROR.getStatusCode()));
    }
  }

  private TCreateSchemaTemplateReq constructTCreateSchemaTemplateReq(
      CreateSchemaTemplateStatement statement) {
    TCreateSchemaTemplateReq req = new TCreateSchemaTemplateReq();
    try {
      Template template = new Template(statement);
      req.setName(template.getName());
      req.setSerializedTemplate(Template.template2ByteBuffer(template));
    } catch (IOException | IllegalPathException e) {
      throw new RuntimeException(e);
    }
    return req;
  }

  @Override
  public List<Template> getAllTemplates() {
    List<Template> templatesList = new ArrayList<>();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      TGetAllTemplatesResp tGetAllTemplatesResp = configNodeClient.getAllTemplates();
      // Get response or throw exception
      if (tGetAllTemplatesResp.getStatus().getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        List<ByteBuffer> list = tGetAllTemplatesResp.getTemplateList();
        Optional<List<ByteBuffer>> optional = Optional.ofNullable(list);
        optional.orElse(new ArrayList<>()).stream()
            .forEach(
                item -> {
                  try {
                    Template template = Template.byteBuffer2Template(item);
                    templatesList.add(template);
                  } catch (IOException | ClassNotFoundException e) {
                    throw new RuntimeException(
                        new IoTDBException(
                            "deserialize template error.",
                            e,
                            TSStatusCode.TEMPLATE_IMCOMPATIBLE.getStatusCode()));
                  }
                });
      } else {
        throw new RuntimeException(
            new IoTDBException(
                tGetAllTemplatesResp.getStatus().getMessage(),
                tGetAllTemplatesResp.getStatus().getCode()));
      }
    } catch (TException | IOException e) {
      throw new RuntimeException(
          new IoTDBException(
              "get all template error.", TSStatusCode.UNDEFINED_TEMPLATE.getStatusCode()));
    }
    return templatesList;
  }

  @Override
  public Template getTemplate(String name) {
    Template template = null;
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      TGetTemplateResp resp = configNodeClient.getTemplate(name);
      if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        byte[] templateBytes = resp.getTemplate();
        if (templateBytes != null && templateBytes.length > 0) {
          template = Template.byteBuffer2Template(ByteBuffer.wrap(templateBytes));
        }
      } else {
        throw new RuntimeException(
            new IoTDBException(resp.status.getMessage(), resp.status.getCode()));
      }
    } catch (Exception e) {
      throw new RuntimeException(
          new IoTDBException(
              "get template info error.", TSStatusCode.UNDEFINED_TEMPLATE.getStatusCode()));
    }
    return template;
  }

  @Override
  public void setSchemaTemplate(String name, PartialPath path) {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      TSetSchemaTemplateReq req = new TSetSchemaTemplateReq();
      req.setName(name);
      req.setPath(path.getFullPath());
      TSStatus tsStatus = configNodeClient.setSchemaTemplate(req);
      if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new RuntimeException(
            new IoTDBException(tsStatus.getMessage(), tsStatus.getCode()));
      }
    } catch (Exception e) {
      throw new RuntimeException(
          new IoTDBException(
              "get schema template error.", TSStatusCode.UNDEFINED_TEMPLATE.getStatusCode()));
    }
  }

  @Override
  public List<PartialPath> getPathsSetTemplate(String name) {
    List<PartialPath> listPath = new ArrayList<PartialPath>();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      TGetPathsSetTemplatesResp resp = configNodeClient.getPathsSetTemplate(name);
      if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        if(resp.getPathList()!=null) {
          resp.getPathList().stream().forEach(item -> {
            try {
              listPath.add(new PartialPath(item));
            }catch (IllegalPathException e){
              e.printStackTrace();
            }
          });
        }
      } else {
        throw new RuntimeException(
            new IoTDBException(resp.status.getMessage(), resp.status.getCode()));
      }
    } catch (Exception e) {
      throw new RuntimeException(
          new IoTDBException(
              "get path set template error.", TSStatusCode.UNDEFINED_TEMPLATE.getStatusCode()));
    }
    return listPath;
  }
}
