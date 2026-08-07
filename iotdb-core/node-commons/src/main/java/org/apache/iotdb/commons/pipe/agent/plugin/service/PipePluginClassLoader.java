/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.pipe.agent.plugin.service;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.i18n.PipeMessages;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ClassLoader for a pipe plugin. Uses the standard parent-delegation model.
 *
 * <p>Before attaching any plugin jar/class URLs, it scans plugin artifacts as raw bytes (via {@link
 * JarFile} / filesystem reads) and compares them with resources visible to the parent ClassLoader
 * through {@link ClassLoader#getResourceAsStream(String)}. That check never defines classes into
 * the parent (or this) ClassLoader; only a later explicit {@link Class#forName} loads the plugin
 * entry class.
 */
@ThreadSafe
public class PipePluginClassLoader extends URLClassLoader {

  private static final String CLASS_SUFFIX = ".class";
  private static final String JAR_SUFFIX = ".jar";
  private static final String MODULE_INFO_CLASS = "module-info.class";
  private static final int MAX_REPORTED_CONFLICTS = 20;

  /**
   * If activeInstanceCount is equals to 0, it means that there is no instance using this
   * classloader. This classloader can only be closed when activeInstanceCount is equals to 0.
   */
  @GuardedBy("this")
  private long activeInstanceCount;

  /**
   * If this classloader is marked as deprecated, then this classloader can be closed after all
   * instances that use this classloader are closed.
   */
  @GuardedBy("this")
  private boolean deprecated;

  public PipePluginClassLoader(String libRoot) throws IOException {
    this(libRoot, ClassLoader.getSystemClassLoader());
  }

  PipePluginClassLoader(String libRoot, ClassLoader parent) throws IOException {
    super(new URL[0], parent);
    Objects.requireNonNull(libRoot, PipeMessages.EXCEPTION_LIBROOT_CANNOT_BE_NULL_C22EAC78);
    activeInstanceCount = 0;
    deprecated = false;

    final Path rootPath = SystemFileFactory.INSTANCE.getFile(libRoot).toPath();
    if (!Files.exists(rootPath)) {
      throw new IOException(
          String.format(
              PipeMessages
                  .EXCEPTION_FAILED_TO_LOAD_PIPE_PLUGIN_FROM_ARG_BECAUSE_THE_PATH_DOES_NOT_EXIST_1AD125AD,
              rootPath));
    }

    // Walk once and reuse for conflict check + URL registration.
    final List<Path> pluginFiles;
    try (Stream<Path> pathStream = Files.walk(rootPath)) {
      pluginFiles = pathStream.filter(Files::isRegularFile).collect(Collectors.toList());
    }

    validateNoConflictingClassesWithParent(rootPath, pluginFiles, parent);
    addUrls(pluginFiles);
  }

  /**
   * Scan plugin jars/classes and reject those whose fully-qualified class names already exist on
   * the parent ClassLoader with different bytecode.
   *
   * <p>Implementation constraints:
   *
   * <ul>
   *   <li>Never call {@code Class.forName} / {@code loadClass} for plugin classes.
   *   <li>Never add plugin URLs to the parent ClassLoader.
   *   <li>Only read bytes via {@link JarFile} / {@link Files} and {@link
   *       ClassLoader#getResourceAsStream(String)}.
   * </ul>
   */
  static void validateNoConflictingClassesWithParent(
      Path rootPath, List<Path> pluginFiles, ClassLoader parent) throws IOException {
    final List<String> conflicts = new ArrayList<>();

    for (Path path : pluginFiles) {
      final String fileName = path.getFileName().toString().toLowerCase(Locale.ROOT);
      if (fileName.endsWith(JAR_SUFFIX)) {
        collectJarConflicts(path, parent, conflicts);
      } else if (fileName.endsWith(CLASS_SUFFIX)) {
        collectLooseClassConflict(rootPath, path, parent, conflicts);
      }
    }

    if (!conflicts.isEmpty()) {
      final String reported =
          conflicts.stream().limit(MAX_REPORTED_CONFLICTS).collect(Collectors.joining(", "));
      throw new IOException(
          String.format(
              PipeMessages
                  .EXCEPTION_FAILED_TO_LOAD_PIPE_PLUGIN_FROM_ARG_BECAUSE_THE_FOLLOWING_CLASSES_CONFLICT_WITH_THE_PARENT_CLASSLOADER_SAME_FULLY_QUALIFIED_NAME_BUT_DIFFERENT_BYTECODE_ARG_0647E8F3,
              rootPath,
              reported));
    }
  }

  private static void collectJarConflicts(Path jarPath, ClassLoader parent, List<String> conflicts)
      throws IOException {
    try (JarFile jarFile =
        new JarFile(jarPath.toFile(), true, JarFile.OPEN_READ, Runtime.version())) {
      final Enumeration<JarEntry> entries = jarFile.entries();
      while (entries.hasMoreElements()) {
        final JarEntry entry = entries.nextElement();
        if (entry.isDirectory()
            || entry.getName().startsWith("META-INF/versions/")
            || !isComparableClassEntry(entry.getName())) {
          continue;
        }
        // Resolve the logical entry through the runtime-aware view of a multi-release JAR.
        final JarEntry runtimeEntry = jarFile.getJarEntry(entry.getName());
        try (InputStream pluginIn = jarFile.getInputStream(runtimeEntry)) {
          maybeAddConflict(entry.getName(), readAllBytes(pluginIn), parent, conflicts);
        }
      }
    }
  }

  private static void collectLooseClassConflict(
      Path libRoot, Path classFile, ClassLoader parent, List<String> conflicts) throws IOException {
    final Path relative = libRoot.relativize(classFile);
    final String resourceName = relative.toString().replace('\\', '/');
    if (!isComparableClassEntry(resourceName)) {
      return;
    }
    maybeAddConflict(resourceName, Files.readAllBytes(classFile), parent, conflicts);
  }

  private static void maybeAddConflict(
      String resourceName, byte[] pluginBytes, ClassLoader parent, List<String> conflicts)
      throws IOException {
    // getResourceAsStream locates parent classpath bytes without defining the Class.
    try (InputStream parentIn = parent.getResourceAsStream(resourceName)) {
      if (parentIn == null) {
        return;
      }
      final byte[] parentBytes = readAllBytes(parentIn);
      if (!Arrays.equals(parentBytes, pluginBytes)) {
        conflicts.add(resourceNameToClassName(resourceName));
      }
    }
  }

  private static boolean isComparableClassEntry(String resourceName) {
    if (!resourceName.endsWith(CLASS_SUFFIX)) {
      return false;
    }
    final int lastSlashIndex = resourceName.lastIndexOf('/');
    final String simpleName =
        lastSlashIndex >= 0
            ? resourceName.substring(lastSlashIndex + 1).toLowerCase(Locale.ROOT)
            : resourceName.toLowerCase(Locale.ROOT);
    return !MODULE_INFO_CLASS.equals(simpleName);
  }

  private static String resourceNameToClassName(String resourceName) {
    return resourceName
        .substring(0, resourceName.length() - CLASS_SUFFIX.length())
        .replace('/', '.');
  }

  private static byte[] readAllBytes(InputStream inputStream) throws IOException {
    return inputStream.readAllBytes();
  }

  private void addUrls(List<Path> pluginFiles) throws IOException {
    for (Path path : pluginFiles) {
      super.addURL(path.toUri().toURL());
    }
  }

  public synchronized void acquire() {
    activeInstanceCount++;
  }

  public synchronized void release() throws IOException {
    if (activeInstanceCount > 0) {
      activeInstanceCount--;
    }
    closeIfPossible();
  }

  public synchronized void markAsDeprecated() throws IOException {
    deprecated = true;
    closeIfPossible();
  }

  private void closeIfPossible() throws IOException {
    if (deprecated && activeInstanceCount == 0) {
      close();
    }
  }
}
