package org.apache.iotdb.confignode.procedure.state.pipe;

public enum CreatePipePluginState {
  LOCK,
  CREATE_ON_CONFIG_NODE,
  CREATE_ON_DATA_NODES,
  UNLOCK
}
