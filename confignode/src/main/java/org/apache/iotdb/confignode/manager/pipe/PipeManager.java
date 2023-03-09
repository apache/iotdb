package org.apache.iotdb.confignode.manager.pipe;

import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.pipe.plugin.PipePluginCoordinator;
import org.apache.iotdb.confignode.persistence.pipe.PipeInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeManager.class);

  private final ConfigManager configManager;

  private final PipeInfo pipeInfo;

  private final PipePluginCoordinator pipePluginCoordinator;

  public PipeManager(ConfigManager configManager, PipeInfo pipeInfo) {
    this.configManager = configManager;
    this.pipeInfo = pipeInfo;
    this.pipePluginCoordinator = new PipePluginCoordinator(this, pipeInfo.getPipePluginInfo());
  }

  public ConfigManager getConfigManager() {
    return configManager;
  }

  public PipePluginCoordinator getPipePluginCoordinator() {
    return pipePluginCoordinator;
  }
}
