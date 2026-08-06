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
import java.util.stream.Stream;

public class PipePluginClassLoaderTest {

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

  private static void deleteRecursively(final Path path) throws IOException {
    if (path == null || !Files.exists(path)) {
      return;
    }
    try (final Stream<Path> stream = Files.walk(path)) {
      for (final Path subPath :
          (Iterable<Path>) stream.sorted(Comparator.reverseOrder())::iterator) {
        Files.deleteIfExists(subPath);
      }
    }
  }
}
