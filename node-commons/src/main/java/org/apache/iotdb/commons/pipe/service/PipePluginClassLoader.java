package org.apache.iotdb.commons.pipe.service;

import org.apache.iotdb.commons.file.SystemFileFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PipePluginClassLoader extends URLClassLoader {

  private final String libRoot;

  /**
   * If activeQueriesCount is equals to 0, it means that there is no query using this classloader.
   * This classloader can only be closed when activeQueriesCount is equals to 0.
   */
  private final AtomicLong activeQueriesCount;

  /**
   * If this classloader is marked as deprecated, then this classloader can be closed after all
   * queries that use this classloader are completed.
   */
  private volatile boolean deprecated;

  public PipePluginClassLoader(String libRoot) throws IOException {
    super(new URL[0]);
    this.libRoot = libRoot;
    activeQueriesCount = new AtomicLong(0);
    deprecated = false;
    addURLs();
  }

  private void addURLs() throws IOException {
    try (Stream<Path> pathStream =
        Files.walk(SystemFileFactory.INSTANCE.getFile(libRoot).toPath())) {
      // skip directory
      for (Path path :
          pathStream.filter(path -> !path.toFile().isDirectory()).collect(Collectors.toList())) {
        super.addURL(path.toUri().toURL());
      }
    }
  }

  public void acquire() {
    activeQueriesCount.incrementAndGet();
  }

  public void release() throws IOException {
    activeQueriesCount.decrementAndGet();
    closeIfPossible();
  }

  public void markAsDeprecated() throws IOException {
    deprecated = true;
    closeIfPossible();
  }

  public void closeIfPossible() throws IOException {
    if (deprecated && activeQueriesCount.get() == 0) {
      close();
    }
  }
}
