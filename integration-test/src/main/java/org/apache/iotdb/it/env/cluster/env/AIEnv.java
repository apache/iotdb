package org.apache.iotdb.it.env.cluster.env;

public class AIEnv extends AbstractEnv {
  @Override
  public void initClusterEnvironment() {
    initClusterEnvironment(1, 1);
  }

  @Override
  public void initClusterEnvironment(int configNodesNum, int dataNodesNum) {
    super.initEnvironment(configNodesNum, dataNodesNum, 10000, true);
  }

  @Override
  public void initClusterEnvironment(
      int configNodesNum, int dataNodesNum, int testWorkingRetryCount) {
    super.initEnvironment(configNodesNum, dataNodesNum, testWorkingRetryCount, true);
  }
}
