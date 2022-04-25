package org.apache.iotdb.confignode.procedure;

import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.procedure.StateMachineProcedure;
import org.apache.iotdb.procedure.exception.ProcedureSuspendedException;
import org.apache.iotdb.procedure.exception.ProcedureYieldException;

import java.io.IOException;
import java.util.List;

public class DeleteStorageGroupProcedure
    extends StateMachineProcedure<ConfigNodeProcedureEnv, DeleteStorageGroupState> {

  private List<TStorageGroupSchema> deleteSgSchemaList;

  public DeleteStorageGroupProcedure(List<TStorageGroupSchema> deleteSgSchemaList) {
    super();
    this.deleteSgSchemaList = deleteSgSchemaList;
  }

  @Override
  protected Flow executeFromState(
      ConfigNodeProcedureEnv clusterProcedureEnvironment,
      DeleteStorageGroupState deleteStorageGroupState)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    return null;
  }

  @Override
  protected void rollbackState(
      ConfigNodeProcedureEnv clusterProcedureEnvironment,
      DeleteStorageGroupState deleteStorageGroupState)
      throws IOException, InterruptedException {}

  @Override
  protected DeleteStorageGroupState getState(int stateId) {
    return null;
  }

  @Override
  protected int getStateId(DeleteStorageGroupState deleteStorageGroupState) {
    return deleteStorageGroupState.ordinal();
  }

  @Override
  protected DeleteStorageGroupState getInitialState() {
    return null;
  }
}
