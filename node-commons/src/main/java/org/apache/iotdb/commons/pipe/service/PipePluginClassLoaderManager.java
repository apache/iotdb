package org.apache.iotdb.commons.pipe.service;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;

import java.io.IOException;

public class PipePluginClassLoaderManager implements IService {

  private final String libRoot;

  /**
   * activeClassLoader is used to load all classes under libRoot. libRoot may be updated before the
   * user executes CREATE PIPEPLUGIN or after the user executes DROP PIPEPLUGIN. Therefore, we need
   * to continuously maintain the activeClassLoader so that the classes it loads are always
   * up-to-date.
   */
  private volatile PipePluginClassLoader activeClassLoader;

  private PipePluginClassLoaderManager(String libRoot) throws IOException {
    this.libRoot = libRoot;
    activeClassLoader = new PipePluginClassLoader(libRoot);
  }

  public PipePluginClassLoader updateAndGetActiveClassLoader() throws IOException {
    PipePluginClassLoader deprecatedClassLoader = activeClassLoader;
    activeClassLoader = new PipePluginClassLoader(libRoot);
    if (deprecatedClassLoader != null) {
      deprecatedClassLoader.markAsDeprecated();
    }
    return activeClassLoader;
  }

  public PipePluginClassLoader getActiveClassLoader() {
    return activeClassLoader;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // IService
  /////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public void start() throws StartupException {
    try {
      SystemFileFactory.INSTANCE.makeDirIfNecessary(libRoot);
      activeClassLoader = new PipePluginClassLoader(libRoot);
    } catch (IOException e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    // nothing to do
  }

  @Override
  public ServiceType getID() {
    return ServiceType.PIPE_PLUGIN_CLASSLOADER_MANAGER_SERVICE;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // singleton instance holder
  /////////////////////////////////////////////////////////////////////////////////////////////////

  private static PipePluginClassLoaderManager INSTANCE = null;

  public static synchronized PipePluginClassLoaderManager getInstance(String libRoot)
      throws IOException {
    if (INSTANCE == null) {
      INSTANCE = new PipePluginClassLoaderManager(libRoot);
    }
    return INSTANCE;
  }

  public static PipePluginClassLoaderManager getInstance() {
    return INSTANCE;
  }
}
