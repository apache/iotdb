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
package org.apache.iotdb.relational.it.schema.fault;

import org.apache.iotdb.it.env.cluster.config.MppJVMConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;

import org.awaitility.Awaitility;
import org.objectweb.asm.ClassReader;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Owns the agent's temporary jar, launch environment and bounded loopback control connections. */
public final class MetadataFaultController implements AutoCloseable {
  private final Path directory;
  private final List<Node> nodes = new ArrayList<>();
  private final MppJVMConfig dataNodeJvmConfig;
  private final List<String> previousJvmOptions;
  private boolean launchEnvironmentRestored;

  public MetadataFaultController(MppJVMConfig dataNodeJvmConfig) throws Exception {
    this.dataNodeJvmConfig = dataNodeJvmConfig;
    previousJvmOptions = new ArrayList<>(dataNodeJvmConfig.getJvmOptions());
    directory = Files.createTempDirectory(Paths.get("target").toAbsolutePath(), "metadata-fault-");
    Path jar = directory.resolve("agent.jar");
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().putValue("Premain-Class", MetadataFaultAgent.class.getName());
    try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar), manifest)) {
      addClass(output, MetadataFaultAgent.class);
      // Keep ASM inside the temporary agent so child-process classpaths need no test artifacts.
      Path asm =
          Paths.get(ClassReader.class.getProtectionDomain().getCodeSource().getLocation().toURI());
      try (JarFile source = new JarFile(asm.toFile())) {
        for (JarEntry entry : source.stream().collect(Collectors.toList())) {
          if (entry.getName().startsWith("org/objectweb/asm/") && !entry.isDirectory()) {
            output.putNextEntry(new JarEntry(entry.getName()));
            try (InputStream input = source.getInputStream(entry)) {
              copy(input, output);
            }
            output.closeEntry();
          }
        }
      }
    }
    List<String> launchOptions = new ArrayList<>(previousJvmOptions);
    launchOptions.add("-javaagent:" + jar + "=" + directory);
    dataNodeJvmConfig.setJvmOptions(launchOptions);
  }

  /** Called immediately after cluster startup, also on startup failure. */
  public void restoreLaunchEnvironment() {
    if (launchEnvironmentRestored) {
      return;
    }
    dataNodeJvmConfig.setJvmOptions(previousJvmOptions);
    launchEnvironmentRestored = true;
  }

  public Node connect(DataNodeWrapper wrapper) throws Exception {
    long pid = wrapper.getInstance().pid();
    Path endpoint = directory.resolve(pid + ".port");
    Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> Files.isRegularFile(endpoint));
    Node node = new Node(Integer.parseInt(Files.readAllLines(endpoint).get(0)));
    nodes.add(node);
    return node;
  }

  private static void addClass(JarOutputStream output, Class<?> type) throws IOException {
    String name = type.getName().replace('.', '/') + ".class";
    output.putNextEntry(new JarEntry(name));
    try (InputStream input = type.getClassLoader().getResourceAsStream(name)) {
      if (input == null) {
        throw new IOException("Missing agent class: " + name);
      }
      copy(input, output);
    }
    output.closeEntry();
    for (Class<?> nested : type.getDeclaredClasses()) {
      addClass(output, nested);
    }
  }

  private static void copy(InputStream input, JarOutputStream output) throws IOException {
    byte[] buffer = new byte[8_192];
    int length;
    while ((length = input.read(buffer)) != -1) {
      output.write(buffer, 0, length);
    }
  }

  /** Release all in-flight calls before shutting down the cluster. */
  public void reset() throws IOException {
    IOException failure = null;
    for (Node node : nodes) {
      try {
        node.reset();
      } catch (IOException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  /** Called after the child processes have stopped. */
  @Override
  public void close() throws IOException {
    restoreLaunchEnvironment();
    try (Stream<Path> paths = Files.walk(directory)) {
      for (Path path : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
        Files.delete(path);
      }
    }
  }

  public static final class Node {
    private final int port;

    private Node(int port) {
      this.port = port;
    }

    public void arm(String hook, String mode, String database) throws IOException {
      arm(hook, mode, database, "");
    }

    public void arm(String hook, String mode, String database, String attributes)
        throws IOException {
      request("arm", hook, mode, database, attributes);
    }

    public Map<String, String> status(String hook) throws IOException {
      return request("status", hook, "", "");
    }

    public void release(String hook) throws IOException {
      request("release", hook, "", "");
    }

    public void reset() throws IOException {
      request("reset", "", "", "");
    }

    public Map<String, String> lease() throws IOException {
      return request("lease", "", "", "");
    }

    public Map<String, String> updaters(String database) throws IOException {
      return request("updaters", "", "", database);
    }

    public long count(String hook) throws IOException {
      return Long.parseLong(request("count", hook, "", "").get("count"));
    }

    public void awaitHeld(String hook) {
      Awaitility.await()
          .atMost(30, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                Map<String, String> state = status(hook);
                if (!Boolean.parseBoolean(state.get("held"))) {
                  throw new AssertionError(hook + " did not reach the gate: " + state);
                }
              });
    }

    private Map<String, String> request(String command, String hook, String mode, String match)
        throws IOException {
      return request(command, hook, mode, match, "");
    }

    private Map<String, String> request(
        String command, String hook, String mode, String match, String attributes)
        throws IOException {
      try (Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 5_000);
        socket.setSoTimeout(5_000);
        try (DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            DataInputStream input = new DataInputStream(socket.getInputStream())) {
          output.writeUTF(command);
          output.writeUTF(hook);
          output.writeUTF(mode);
          output.writeUTF(match);
          output.writeUTF(attributes);
          output.flush();
          Map<String, String> result = new HashMap<>();
          int size = input.readInt();
          for (int i = 0; i < size; i++) {
            result.put(input.readUTF(), input.readUTF());
          }
          if (result.containsKey("error")) {
            throw new IOException("Metadata IT controller: " + result.get("error"));
          }
          return result;
        }
      }
    }
  }
}
