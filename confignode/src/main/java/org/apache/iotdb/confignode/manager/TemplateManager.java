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
