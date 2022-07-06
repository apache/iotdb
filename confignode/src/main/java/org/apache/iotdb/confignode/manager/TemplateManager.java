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

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.persistence.TemplateInfo;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chenhuangyun
 * @date 2022/6/28
 */
public class TemplateManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TemplateManager.class);

  private final IManager configManager;
  private final TemplateInfo templateInfo;

  public TemplateManager(IManager configManager, TemplateInfo templateInfo) {
    this.configManager = configManager;
    this.templateInfo = templateInfo;
  }

  public TSStatus createTemplate(CreateSchemaTemplatePlan createSchemaTemplatePlan) {
    try {
      TSStatus tsStatus = getConsensusManager().write(createSchemaTemplatePlan).getStatus();
      return tsStatus;
    } catch (Exception e) {
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage("schema template failed to create  because some error.");
    }
  }

  public TGetAllTemplatesResp getAllTemplates() {
    return templateInfo.getAllTemplate();
  }

  TGetTemplateResp getTemplate(String req) {
    return templateInfo.getMatchedTemplateByName(req);
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }
}
