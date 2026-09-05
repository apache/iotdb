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

import org.junit.Assert;
import org.junit.Test;

import javax.tools.JavaCompiler;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Stream;

public class PipePluginClassLoaderTest {

  // Verify that a plugin is rejected when it contains different bytecode for a parent class.
  @Test
  public void testRejectPluginWhenParentHasDifferentBytecode() throws Exception {
    final Path tempDir = Files.createTempDirectory("pipe-plugin-classloader-conflict");
    try {
      final Path parentJar = buildJarWithHelper(tempDir, "parent", "parent");
      final Path childJar = buildJarWithHelper(tempDir, "child", "child");

      try (final URLClassLoader parentClassLoader =
          new URLClassLoader(new URL[] {parentJar.toUri().toURL()}, null)) {
        // Ensure parent has already resolved the class resource.
        Assert.assertNotNull(parentClassLoader.getResource("test/dep/Helper.class"));

        try {
          new PipePluginClassLoader(childJar.toString(), parentClassLoader);
          Assert.fail("Expected IOException for conflicting classes");
        } catch (final IOException e) {
          Assert.assertTrue(e.getMessage().contains("test.dep.Helper"));
        }

        // Conflict check must not define classes into the parent ClassLoader.
        Assert.assertNull(findLoadedClass(parentClassLoader, "test.dep.Helper"));
        Assert.assertNull(findLoadedClass(parentClassLoader, "test.plugin.Sample"));
      }
    } finally {
      deleteRecursively(tempDir);
    }
  }

  // Verify that identical parent and plugin bytecode is allowed and uses parent delegation.
  @Test
  public void testAllowPluginWhenParentHasIdenticalBytecode() throws Exception {
    final Path tempDir = Files.createTempDirectory("pipe-plugin-classloader-same");
    try {
      final Path sharedClasses = Files.createDirectory(tempDir.resolve("shared-classes"));
      final Path sharedSources = Files.createDirectory(tempDir.resolve("shared-sources"));
      compile(
          sharedSources,
          sharedClasses,
          createSources(
              "package test.dep;"
                  + "public class Helper {"
                  + "  public static String value() { return \"same\"; }"
                  + "}",
              true),
          createSources(
              "package test.plugin;"
                  + "public class Sample {"
                  + "  public String ping() { return test.dep.Helper.value(); }"
                  + "}",
              false));

      final Path parentJar = tempDir.resolve("parent.jar");
      final Path childJar = tempDir.resolve("child.jar");
      createJar(parentJar, sharedClasses, Arrays.asList("test/dep/Helper.class"));
      createJar(
          childJar,
          sharedClasses,
          Arrays.asList("test/plugin/Sample.class", "test/dep/Helper.class"));

      try (final URLClassLoader parentClassLoader =
              new URLClassLoader(new URL[] {parentJar.toUri().toURL()}, null);
          final PipePluginClassLoader pluginClassLoader =
              new PipePluginClassLoader(childJar.toString(), parentClassLoader)) {
        final Class<?> sampleClass = Class.forName("test.plugin.Sample", true, pluginClassLoader);
        // Sample is only in the plugin jar → loaded by plugin ClassLoader.
        Assert.assertSame(pluginClassLoader, sampleClass.getClassLoader());
        // Helper is identical and present on parent → parent-delegation loads parent's copy.
        final Class<?> helperClass = Class.forName("test.dep.Helper", true, pluginClassLoader);
        Assert.assertSame(parentClassLoader, helperClass.getClassLoader());
        final Object sample = sampleClass.getDeclaredConstructor().newInstance();
        Assert.assertEquals("same", sampleClass.getMethod("ping").invoke(sample));
      }
    } finally {
      deleteRecursively(tempDir);
    }
  }

  // Verify that conflict scanning does not load plugin classes into the parent loader.
  @Test
  public void testConflictCheckDoesNotLoadPluginClasses() throws Exception {
    final Path tempDir = Files.createTempDirectory("pipe-plugin-classloader-noload");
    try {
      final Path parentJar = buildJarWithHelper(tempDir, "parent", "parent");
      final Path childJar = buildJarWithHelper(tempDir, "child", "child");

      try (final URLClassLoader parentClassLoader =
          new URLClassLoader(new URL[] {parentJar.toUri().toURL()}, null)) {
        try {
          PipePluginClassLoader.validateNoConflictingClassesWithParent(
              childJar, List.of(childJar), parentClassLoader);
          Assert.fail("Expected IOException for conflicting classes");
        } catch (final IOException expected) {
          // expected
        }

        Assert.assertNull(findLoadedClass(parentClassLoader, "test.dep.Helper"));
        Assert.assertNull(findLoadedClass(parentClassLoader, "test.plugin.Sample"));
      }
    } finally {
      deleteRecursively(tempDir);
    }
  }

  // Verify that Java core classes cannot be overridden by plugin classes.
  @Test
  public void testJavaCoreClassIsLoadedByBootstrapClassLoader() throws Exception {
    // Verify that a plugin cannot replace a Java core class through parent delegation.
    final Path tempDir = Files.createTempDirectory("pipe-plugin-core-protect");
    try {
      final Path childJar = tempDir.resolve("plugin.jar");
      createJarWithResource(childJar, "java/lang/String.class", new byte[0]);

      try (final URLClassLoader parentClassLoader = new URLClassLoader(new URL[0], null)) {
        try {
          new PipePluginClassLoader(childJar.toString(), parentClassLoader);
          Assert.fail("Expected IOException for a conflicting Java core class");
        } catch (IOException expected) {
          Assert.assertTrue(expected.getMessage().contains("java.lang.String"));
        }
      }
    } finally {
      deleteRecursively(tempDir);
    }
  }

  // Verify that closing the plugin loader releases the plugin JAR file handle.
  @Test
  public void testPluginJarFileHandleReleasedAfterClose() throws Exception {
    // Verify that closing the plugin class loader releases the underlying JAR file.
    final Path tempDir = Files.createTempDirectory("pipe-plugin-file-handle");
    try {
      final Path childJar = buildJarWithHelper(tempDir, "close-test", "dummy");
      try (final URLClassLoader parentClassLoader = new URLClassLoader(new URL[0], null);
          final PipePluginClassLoader pluginClassLoader =
              new PipePluginClassLoader(childJar.toString(), parentClassLoader)) {
        Class.forName("test.plugin.Sample", true, pluginClassLoader);
        pluginClassLoader.close();
      }
      Assert.assertTrue(Files.deleteIfExists(childJar));
    } finally {
      deleteRecursively(tempDir);
    }
  }

  // Verify parent-first resource lookup and enumeration of duplicate resources.
  @Test
  public void testPluginResourceIsolation() throws Exception {
    // Verify parent-first lookup and enumeration of duplicate resources.
    final Path tempDir = Files.createTempDirectory("pipe-plugin-resource-isolation");
    try {
      final Path parentJar = tempDir.resolve("parent.jar");
      final Path childJar = tempDir.resolve("child.jar");
      createJarWithResource(parentJar, "config.properties", "source=parent");
      createJarWithResource(childJar, "config.properties", "source=child");
      try (final URLClassLoader parentClassLoader =
              new URLClassLoader(new URL[] {parentJar.toUri().toURL()}, null);
          final PipePluginClassLoader pluginClassLoader =
              new PipePluginClassLoader(childJar.toString(), parentClassLoader)) {
        final URL resourceUrl = pluginClassLoader.getResource("config.properties");
        Assert.assertNotNull(resourceUrl);
        try (InputStream inputStream = resourceUrl.openStream()) {
          Assert.assertEquals(
              "source=parent", new String(inputStream.readAllBytes(), StandardCharsets.UTF_8));
        }
        final List<URL> allResources = new ArrayList<>();
        pluginClassLoader
            .getResources("config.properties")
            .asIterator()
            .forEachRemaining(allResources::add);
        Assert.assertEquals(2, allResources.size());
      }
    } finally {
      deleteRecursively(tempDir);
    }
  }

  // Verify runtime-selected Multi-Release JAR entries are compared consistently.
  @Test
  public void testMultiReleaseJarConflictDetectionUsesRuntimeVersion() throws Exception {
    final Path tempDir = Files.createTempDirectory("pipe-plugin-multi-release");
    try {
      final Path parentJar = tempDir.resolve("parent.jar");
      final Path samePluginJar = tempDir.resolve("same-plugin.jar");
      final Path differentPluginJar = tempDir.resolve("different-plugin.jar");
      createMultiReleaseJar(parentJar, "same");
      createMultiReleaseJar(samePluginJar, "same");
      createMultiReleaseJar(differentPluginJar, "different");

      try (final URLClassLoader parentClassLoader =
          new URLClassLoader(new URL[] {parentJar.toUri().toURL()}, null)) {
        try (final PipePluginClassLoader ignored =
            new PipePluginClassLoader(samePluginJar.toString(), parentClassLoader)) {
          // Identical runtime-selected bytes must not be reported as a conflict.
        }
        try {
          new PipePluginClassLoader(differentPluginJar.toString(), parentClassLoader);
          Assert.fail("Expected a conflict for different runtime-selected bytes");
        } catch (IOException expected) {
          Assert.assertTrue(expected.getMessage().contains("test.dep.Helper"));
        }
      }
    } finally {
      deleteRecursively(tempDir);
    }
  }

  private static Path buildJarWithHelper(Path tempDir, String prefix, String helperValue)
      throws IOException {
    final Path sources = Files.createDirectory(tempDir.resolve(prefix + "-sources"));
    final Path classes = Files.createDirectory(tempDir.resolve(prefix + "-classes"));
    compile(
        sources,
        classes,
        createSources(
            "package test.dep;"
                + "public class Helper {"
                + "  public static String value() { return \""
                + helperValue
                + "\"; }"
                + "}",
            true),
        createSources(
            "package test.plugin;"
                + "public class Sample {"
                + "  public String ping() { return test.dep.Helper.value(); }"
                + "}",
            false));
    final Path jar = tempDir.resolve(prefix + ".jar");
    createJar(jar, classes, Arrays.asList("test/plugin/Sample.class", "test/dep/Helper.class"));
    return jar;
  }

  private static Class<?> findLoadedClass(ClassLoader classLoader, String name) throws Exception {
    final Method method = ClassLoader.class.getDeclaredMethod("findLoadedClass", String.class);
    method.setAccessible(true);
    return (Class<?>) method.invoke(classLoader, name);
  }

  private static Map<String, String> createSources(
      final String source, final boolean helperSource) {
    final Map<String, String> sources = new LinkedHashMap<>();
    sources.put(helperSource ? "test.dep.Helper" : "test.plugin.Sample", source);
    return sources;
  }

  private static void compile(
      final Path sourcesDir, final Path classesDir, final Map<String, String>... sourceGroups)
      throws IOException {
    final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    Assert.assertNotNull("A JDK is required to run this test.", compiler);

    final List<File> sourceFiles = new ArrayList<>();
    for (final Map<String, String> sourceGroup : sourceGroups) {
      for (final Map.Entry<String, String> entry : sourceGroup.entrySet()) {
        final Path sourceFile = sourcesDir.resolve(entry.getKey().replace('.', '/') + ".java");
        Files.createDirectories(sourceFile.getParent());
        Files.write(sourceFile, entry.getValue().getBytes(StandardCharsets.UTF_8));
        sourceFiles.add(sourceFile.toFile());
      }
    }

    try (final StandardJavaFileManager fileManager =
        compiler.getStandardFileManager(null, null, StandardCharsets.UTF_8)) {
      final boolean success =
          compiler
              .getTask(
                  null,
                  fileManager,
                  null,
                  Arrays.asList("-d", classesDir.toString()),
                  null,
                  fileManager.getJavaFileObjectsFromFiles(sourceFiles))
              .call();
      Assert.assertTrue(success);
    }
  }

  private static void createJar(
      final Path jarPath, final Path classesDir, final List<String> classEntries)
      throws IOException {
    try (final JarOutputStream jarOutputStream =
        new JarOutputStream(Files.newOutputStream(jarPath))) {
      for (final String classEntry : classEntries) {
        jarOutputStream.putNextEntry(new JarEntry(classEntry));
        jarOutputStream.write(Files.readAllBytes(classesDir.resolve(classEntry)));
        jarOutputStream.closeEntry();
      }
    }
  }

  private static void createJarWithResource(Path jarPath, String resourceName, String content)
      throws IOException {
    createJarWithResource(jarPath, resourceName, content.getBytes(StandardCharsets.UTF_8));
  }

  private static void createJarWithResource(Path jarPath, String resourceName, byte[] content)
      throws IOException {
    try (JarOutputStream jarOutputStream = new JarOutputStream(Files.newOutputStream(jarPath))) {
      jarOutputStream.putNextEntry(new JarEntry(resourceName));
      jarOutputStream.write(content);
      jarOutputStream.closeEntry();
    }
  }

  private static void createMultiReleaseJar(Path jarPath, String versionedValue)
      throws IOException {
    final Path baseSources = Files.createTempDirectory("mr-base-sources");
    final Path baseClasses = Files.createTempDirectory("mr-base-classes");
    final Path versionSources = Files.createTempDirectory("mr-version-sources");
    final Path versionClasses = Files.createTempDirectory("mr-version-classes");
    try {
      compile(
          baseSources,
          baseClasses,
          createSources(
              "package test.dep; public class Helper { public static String value() { return \"base\"; } }",
              true));
      compile(
          versionSources,
          versionClasses,
          createSources(
              "package test.dep; public class Helper { public static String value() { return \""
                  + versionedValue
                  + "\"; } }",
              true));
      final Manifest manifest = new Manifest();
      manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
      manifest.getMainAttributes().putValue("Multi-Release", "true");
      try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jarPath), manifest)) {
        output.putNextEntry(new JarEntry("test/dep/Helper.class"));
        output.write(Files.readAllBytes(baseClasses.resolve("test/dep/Helper.class")));
        output.closeEntry();
        output.putNextEntry(new JarEntry("META-INF/versions/17/test/dep/Helper.class"));
        output.write(Files.readAllBytes(versionClasses.resolve("test/dep/Helper.class")));
        output.closeEntry();
      }
    } finally {
      deleteRecursively(baseSources);
      deleteRecursively(baseClasses);
      deleteRecursively(versionSources);
      deleteRecursively(versionClasses);
    }
  }

  private static void deleteRecursively(final Path path) throws IOException {
    if (path == null || !Files.exists(path)) {
      return;
    }
    // On Windows a closed URLClassLoader may not release its JAR file handle immediately,
    // so the deletion can transiently fail with a FileSystemException. Retry to avoid
    // flaky failures in the plugin-jar tests.
    IOException lastException = null;
    for (int attempt = 0; attempt < 5; attempt++) {
      try {
        try (final Stream<Path> stream = Files.walk(path)) {
          for (final Path subPath :
              (Iterable<Path>) stream.sorted(Comparator.reverseOrder())::iterator) {
            Files.deleteIfExists(subPath);
          }
        }
        return;
      } catch (final IOException e) {
        lastException = e;
        System.gc();
        try {
          Thread.sleep(100L * (attempt + 1));
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    throw lastException;
  }
}
