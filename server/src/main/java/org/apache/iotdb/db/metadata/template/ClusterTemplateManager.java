package org.apache.iotdb.db.metadata.template;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.confignode.rpc.thrift.TCreateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.client.DataNodeClientPoolFactory;
import org.apache.iotdb.db.exception.metadata.template.SchemaTemplateException;
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

/**
 * @author chenhuangyun
 * @date 2022/6/28
 */
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
      throw new RuntimeException(new SchemaTemplateException("create template error.", e));
    }
  }

  private TCreateSchemaTemplateReq constructTCreateSchemaTemplateReq(
      CreateSchemaTemplateStatement statement) {
    TCreateSchemaTemplateReq req = new TCreateSchemaTemplateReq();
    try {
      Template template = new Template(statement);
      req.setName(template.getName());
      req.setSerializedTemplate(Template.template2ByteBuffer(template));
    } catch (Exception e) {
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
                        new SchemaTemplateException("deserialize template error.", e));
                  }
                });
      } else {
        throw new RuntimeException(new SchemaTemplateException(tGetAllTemplatesResp.getStatus()));
      }
    } catch (TException | IOException e) {
      throw new RuntimeException(new SchemaTemplateException("get all template error.", e));
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
        throw new RuntimeException(new SchemaTemplateException(resp.getStatus()));
      }
    } catch (Exception e) {
      throw new RuntimeException(new SchemaTemplateException("get template info error.", e));
    }
    return template;
  }
}
