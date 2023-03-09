package org.apache.iotdb.db.conf;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.StartupChecks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/** DataNodeStartCheck checks the parameters in iotdb-datanode.properties when start and restart */
public class DataNodeStartCheck extends StartupChecks {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeStartCheck.class);
  private final IoTDBConfig config;

  public DataNodeStartCheck(String nodeRole, IoTDBConfig config) {
    super(nodeRole);
    this.config = config;
  }

  private void checkDataNodePortUnique() throws StartupException {
    Set<Integer> portSet = new HashSet<>();
    int dataNodePort = 6;
    portSet.add(config.getInternalPort());
    portSet.add(config.getMqttPort());
    portSet.add(config.getRpcPort());
    portSet.add(config.getMppDataExchangePort());
    portSet.add(config.getDataRegionConsensusPort());
    portSet.add(config.getSchemaRegionConsensusPort());
    if (portSet.size() != dataNodePort)
      throw new StartupException("ports used in datanode have repeat.");
    else {
      LOGGER.info("DataNode port check successful.");
    }
  }

  protected void portCheck() {
    preChecks.add(this::checkDataNodePortUnique);
  }

  public void startUpCheck() throws StartupException {
    envCheck();
    portCheck();
    verify();
  }
}
