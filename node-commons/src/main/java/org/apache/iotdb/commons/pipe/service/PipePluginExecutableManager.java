package org.apache.iotdb.commons.pipe.service;

import org.apache.iotdb.commons.executable.ExecutableManager;
import org.apache.iotdb.commons.file.SystemFileFactory;

import java.io.File;
import java.io.IOException;

public class PipePluginExecutableManager extends ExecutableManager {

  private static PipePluginExecutableManager INSTANCE = null;

  public PipePluginExecutableManager(String temporaryLibRoot, String libRoot) {
    super(temporaryLibRoot, libRoot);
  }

  public static synchronized PipePluginExecutableManager setupAndGetInstance(
      String temporaryLibRoot, String libRoot) throws IOException {
    if (INSTANCE == null) {
      SystemFileFactory.INSTANCE.makeDirIfNecessary(temporaryLibRoot);
      SystemFileFactory.INSTANCE.makeDirIfNecessary(libRoot);
      SystemFileFactory.INSTANCE.makeDirIfNecessary(libRoot + File.separator + INSTALL_DIR);
      INSTANCE = new PipePluginExecutableManager(temporaryLibRoot, libRoot);
    }
    return INSTANCE;
  }

  public static PipePluginExecutableManager getInstance() {
    return INSTANCE;
  }
}
