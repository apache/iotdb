package org.apache.iotdb.confignode.procedure.impl.model;

import org.apache.iotdb.ainode.rpc.thrift.ITableSchema;
import org.apache.iotdb.ainode.rpc.thrift.TTrainingReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.ainode.AINodeClient;
import org.apache.iotdb.commons.client.ainode.AINodeClientManager;
import org.apache.iotdb.commons.client.ainode.AINodeInfo;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.commons.model.ModelStatus;
import org.apache.iotdb.commons.model.ModelType;
import org.apache.iotdb.confignode.consensus.request.write.model.CreateModelPlan;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.confignode.procedure.exception.ProcedureYieldException;
import org.apache.iotdb.confignode.procedure.impl.node.AbstractNodeProcedure;
import org.apache.iotdb.confignode.procedure.state.model.CreateTrainingState;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TTableInfo;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateModelInfoReq;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CreateTrainingProcedure extends AbstractNodeProcedure<CreateTrainingState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateTrainingProcedure.class);
  private static final int RETRY_THRESHOLD = 0;
  private String modelId;
  private String curDatabase;
  private List<String> targetTables;
  private List<String> targetDbs;
  private boolean useAllData;

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  public CreateTrainingProcedure(
      String modelId,
      String curDatabase,
      List<String> targetTables,
      List<String> targetDbs,
      boolean useAllData) {
    this.modelId = modelId;
    this.curDatabase = curDatabase;
    this.targetTables = targetTables;
    this.targetDbs = targetDbs;
    this.useAllData = useAllData;
  }

  public CreateTrainingProcedure() {
    super();
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, CreateTrainingState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    try {
      TTrainingReq trainingReq = new TTrainingReq();
      trainingReq.setModelId(modelId);
      trainingReq.setModelType((byte) ModelType.USER_DEFINED.ordinal());

      //            if (createTraining.getExistingModelId() != null) {
      //                trainingReq.setExistingModelId(createTraining.getExistingModelId());
      //            }

      //            if (createTraining.getParameters() != null) {
      //                trainingReq.setParameters(createTraining.getParameters());
      //            }

      List<ITableSchema> tableSchemaList = new ArrayList<>();

      TSStatus status =
          env.getConfigManager().getConsensusManager().write(new CreateModelPlan(modelId));
      if (status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new MetadataException("Can't init model " + modelId);
      }
      if (useAllData || !targetDbs.isEmpty()) {
        List<String> databaseNameList = new ArrayList<>();
        if (useAllData) {
          TShowDatabaseResp resp = env.getConfigManager().showDatabase(new TGetDatabaseReq());
          databaseNameList.addAll(resp.getDatabaseInfoMap().keySet());
        } else {
          databaseNameList.addAll(targetDbs);
        }

        for (String database : databaseNameList) {
          TShowTableResp resp = env.getConfigManager().showTables(database, false);
          for (TTableInfo tableInfo : resp.getTableInfoList()) {
            tableSchemaList.add(new ITableSchema(database, tableInfo.getTableName()));
          }
        }
      }
      env.getConfigManager()
          .updateModelInfo(new TUpdateModelInfoReq(modelId, ModelStatus.TRAINING.ordinal()));

      for (String tableName : targetTables) {
        tableSchemaList.add(new ITableSchema(curDatabase, tableName));
      }
      trainingReq.setTargetTables(tableSchemaList);

      try (AINodeClient client =
          AINodeClientManager.getInstance().borrowClient(AINodeInfo.endPoint)) {
        status = client.createTrainingTask(trainingReq);
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new IllegalArgumentException(
              "Something went when training, please check the log of AINode");
        }
      }
    } catch (final Exception e) {
      ModelInformation modelInformation = new ModelInformation(modelId, ModelStatus.UNAVAILABLE);
      modelInformation.setAttribute(e.getMessage());
      //            try{
      //                env.getConfigManager()
      //                        .getConsensusManager()
      //                        .write(new UpdateModelInfoPlan(modelId, modelInformation,
      // Collections.emptyList()));
      //
      //            } catch (Exception e2){
      //                LOGGER.error(e2.getMessage());
      //            }
      if (isRollbackSupported(state)) {
        LOGGER.error("Fail in CreateModelProcedure", e);
        setFailure(new ProcedureException(e.getMessage()));
      } else {
        LOGGER.error("Retrievable error trying to create model [{}], state [{}]", modelId, state);
      }
    }
    return Flow.NO_MORE_STATE;
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv configNodeProcedureEnv, CreateTrainingState createTrainingState)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected boolean isRollbackSupported(CreateTrainingState createTrainingState) {
    return false;
  }

  @Override
  protected CreateTrainingState getState(int stateId) {
    return CreateTrainingState.TRAINING;
  }

  @Override
  protected int getStateId(CreateTrainingState createTrainingState) {
    return 0;
  }

  @Override
  protected CreateTrainingState getInitialState() {
    return CreateTrainingState.TRAINING;
  }
}
