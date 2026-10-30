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

package org.apache.iotdb.it.fault.metadata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.ProtectionDomain;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.LoaderClassPath;
import javassist.expr.ExprEditor;
import javassist.expr.MethodCall;

/**
 * Adds deterministic metadata fault boundaries only to child JVMs started by the recovery ITs.
 * Server sources and artifacts contain no test hooks. The agent is built from test classes at IT
 * setup, so the same setup works under Failsafe and in an IDE, without an installed agent artifact.
 */
public final class MetadataTestAgent {
  private static final String HOOKS = MetadataTestHooks.class.getName();
  private static final String STATE_MACHINE =
      "org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure";
  private static final String SCHEMA_STATE_MACHINE =
      "org.apache.iotdb.db.consensus.statemachine.schemaregion.SchemaRegionStateMachine";
  private static final String RATIS_CONSENSUS = "org.apache.iotdb.consensus.ratis.RatisConsensus";
  private static final String CONFIG_CLIENT =
      "org.apache.iotdb.db.protocol.client.ConfigNodeClient";
  private static final String ABSTRACT_CONFIG_CLIENT =
      "org.apache.iotdb.commons.client.AbstractConfigNodeClient";
  private static final String INTERNAL_RPC =
      "org.apache.iotdb.db.protocol.thrift.impl.DataNodeInternalRPCServiceImpl";
  private static final String STATUS_CODES = "org.apache.iotdb.rpc.TSStatusCode.";
  private static final Set<String> TARGETS =
      new HashSet<>(
          Arrays.asList(
              STATE_MACHINE,
              SCHEMA_STATE_MACHINE,
              RATIS_CONSENSUS,
              CONFIG_CLIENT,
              ABSTRACT_CONFIG_CLIENT,
              INTERNAL_RPC));

  private MetadataTestAgent() {}

  public static void premain(final String arguments, final Instrumentation instrumentation)
      throws IOException {
    MetadataTestHooks.agentStarted();
    instrumentation.addTransformer(new Transformer());
  }

  /** The dependency is test-scoped and bundled only into this temporary agent JAR. */
  public static synchronized Path createJar(final Path destination) throws Exception {
    final Path target = destination.toAbsolutePath();
    Files.createDirectories(target.getParent());
    final Path temporary = Files.createTempFile(target.getParent(), "metadata-agent-", ".jar");
    final Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().putValue("Premain-Class", MetadataTestAgent.class.getName());
    try {
      try (JarOutputStream output =
              new JarOutputStream(Files.newOutputStream(temporary), manifest);
          JarFile dependency =
              new JarFile(
                  Paths.get(
                          ClassPool.class
                              .getProtectionDomain()
                              .getCodeSource()
                              .getLocation()
                              .toURI())
                      .toFile())) {
        addClass(output, MetadataTestAgent.class);
        addClass(output, MetadataTestHooks.class);
        for (JarEntry entry : java.util.Collections.list(dependency.entries())) {
          if (!entry.isDirectory() && entry.getName().startsWith("javassist/")) {
            output.putNextEntry(new JarEntry(entry.getName()));
            try (InputStream input = dependency.getInputStream(entry)) {
              input.transferTo(output);
            }
            output.closeEntry();
          }
        }
      }
      Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
      return target;
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  private static void addClass(final JarOutputStream output, final Class<?> type)
      throws IOException {
    final String resource = type.getName().replace('.', '/') + ".class";
    output.putNextEntry(new JarEntry(resource));
    try (InputStream input = type.getResourceAsStream("/" + resource)) {
      if (input == null) {
        throw new IOException("Missing test agent class: " + resource);
      }
      input.transferTo(output);
    }
    output.closeEntry();
    for (Class<?> nested : type.getDeclaredClasses()) {
      addClass(output, nested);
    }
  }

  private static String errorStatus(final String code) {
    return "new org.apache.iotdb.common.rpc.thrift.TSStatus("
        + STATUS_CODES
        + code
        + ".getStatusCode())";
  }

  private static final class Transformer implements ClassFileTransformer {
    @Override
    public byte[] transform(
        final ClassLoader loader,
        final String name,
        final Class<?> redefined,
        final ProtectionDomain domain,
        final byte[] original) {
      if (name == null || !TARGETS.contains(name.replace('/', '.'))) {
        return null;
      }
      CtClass type = null;
      try {
        final ClassPool pool = new ClassPool(false);
        pool.appendSystemPath();
        pool.insertClassPath(new LoaderClassPath(loader));
        type = pool.makeClass(new ByteArrayInputStream(original));
        switch (type.getName()) {
          case STATE_MACHINE:
            final ProcedureBarrier barrier = new ProcedureBarrier();
            type.getDeclaredMethod("execute").instrument(barrier);
            if (barrier.matches != 1) {
              throw new CannotCompileException("Expected exactly one procedure state transition");
            }
            break;
          case SCHEMA_STATE_MACHINE:
            snapshotBoundaries(type);
            break;
          case RATIS_CONSENSUS:
            // The production default needs 1024 entries before purging. Keep the real purge and
            // snapshot-install paths, with a small threshold only in these child test JVMs.
            type.getDeclaredConstructors()[0].insertAfter(
                "{ if ($1.getConsensusGroupType() == "
                    + "org.apache.iotdb.common.rpc.thrift.TConsensusGroupType.SchemaRegion) "
                    + "org.apache.ratis.server.RaftServerConfigKeys$Log.setPurgeGap(properties, 1); }");
            break;
          case CONFIG_CLIENT:
            failRpc(type, "reloadCacheAfterLeaseRecovery", MetadataTestHooks.LEASE_RECOVERY);
            break;
          case ABSTRACT_CONFIG_CLIENT:
            failRpc(type, "executeRemoteCallWithRetry", MetadataTestHooks.CONFIG_NODE_RPC);
            break;
          case INTERNAL_RPC:
            internalRpcBoundaries(type, pool);
            break;
          default:
            throw new AssertionError(type.getName());
        }
        final byte[] transformed = type.toBytecode();
        MetadataTestHooks.transformed(type.getName());
        return transformed;
      } catch (Exception e) {
        MetadataTestHooks.transformationFailed(name, e);
        // Instrumentation normally ignores transformer exceptions and loads the original class.
        // Reject this class instead: an IT must never silently run without its fault boundaries.
        return new byte[] {0};
      } finally {
        if (type != null) {
          type.detach();
        }
      }
    }

    private static void failRpc(final CtClass type, final String method, final String point)
        throws Exception {
      type.getDeclaredMethod(method)
          .insertBefore(
              "{ if ("
                  + HOOKS
                  + ".fail(\""
                  + point
                  + "\"))"
                  + " throw new org.apache.thrift.TException(); }");
    }

    private static void internalRpcBoundaries(final CtClass type, final ClassPool pool)
        throws Exception {
      for (String method :
          Arrays.asList(
              "invalidatePartitionCache",
              "invalidateSchemaCache",
              "invalidateMatchedSchemaCache",
              "updateTable",
              "invalidateTableCache",
              "invalidateColumnCache")) {
        type.getDeclaredMethod(method)
            .insertBefore(
                "{ if ("
                    + HOOKS
                    + ".fail(\""
                    + MetadataTestHooks.CONFIG_NODE_RPC
                    + "\")) {"
                    + HOOKS
                    + ".observe(\"cache-broadcast-rejected\", \"rejected\"); return "
                    + errorStatus("CAN_NOT_CONNECT_DATANODE")
                    + "; } }");
      }
      type.getDeclaredMethod("getDataNodeHeartBeat")
          .insertBefore(
              "{ if ("
                  + HOOKS
                  + ".fail(\""
                  + MetadataTestHooks.CONFIG_NODE_RPC
                  + "\")) {"
                  + HOOKS
                  + ".observe(\"heartbeat-rejected\", \"rejected\");"
                  + " throw new org.apache.thrift.TException(); } }");
      final CtMethod update = type.getDeclaredMethod("updateAttribute");
      update.addLocalVariable("metadataTestInvocation", pool.get("java.lang.String"));
      update.insertBefore(
          "{ metadataTestInvocation = null; if ("
              + HOOKS
              + ".fail(\""
              + MetadataTestHooks.ATTRIBUTE_UPDATE
              + "\")) return "
              + errorStatus("EXECUTE_STATEMENT_ERROR")
              + ";"
              + " metadataTestInvocation = "
              + HOOKS
              + ".captureAttributeUpdate();"
              + " if (metadataTestInvocation == null) return "
              + errorStatus("EXECUTE_STATEMENT_ERROR")
              + "; }");
      update.insertAfter(
          "{ if (metadataTestInvocation != null && $_.getCode() == "
              + STATUS_CODES
              + "SUCCESS_STATUS.getStatusCode()) "
              + HOOKS
              + ".observe(\"attribute-update-returned.\" + metadataTestInvocation,"
              + " \"delivered\"); }");
    }

    private static void snapshotBoundaries(final CtClass type) throws Exception {
      final String region = "schemaRegion.getSchemaRegionId().getId()";
      final String reject = HOOKS + ".fail(\"snapshot-create.\" + " + region + ")";
      final CtMethod snapshot = type.getDeclaredMethod("takeSnapshot");
      snapshot.insertBefore("{ if (" + reject + ") return false; }");
      snapshot.insertAfter(
          "{ if ($_ && "
              + reject
              + ") $_ = false; if ($_) "
              + HOOKS
              + ".observe(\""
              + MetadataTestHooks.SNAPSHOT_CREATED
              + ".\" + "
              + region
              + ", $1.getName(), $1.getAbsolutePath()); }");
      type.getDeclaredMethod("loadSnapshot")
          .insertAfter(
              "{ if ($_) "
                  + HOOKS
                  + ".observe(\""
                  + MetadataTestHooks.SNAPSHOT_LOADED
                  + ".\" + "
                  + region
                  + ", $1.getName(), $1.getAbsolutePath()); }");
    }
  }

  private static final class ProcedureBarrier extends ExprEditor {
    private int matches;

    @Override
    public void edit(final MethodCall call) throws CannotCompileException {
      if ("executeFromState".equals(call.getMethodName())) {
        matches++;
        // The executor persisted this scheduled state before invoking execute(). Keep the pause
        // immediately before the real transition, not at an estimated time after the DDL starts.
        call.replace(
            "{ if ("
                + HOOKS
                + ".procedure($0, $2)) {"
                + " setFailure(new org.apache.iotdb.confignode.procedure.exception.ProcedureException("
                + "new java.io.IOException())); $_ = "
                + STATE_MACHINE
                + "$Flow.HAS_MORE_STATE;"
                + " } else { $_ = $proceed($$); } }");
      }
    }
  }
}
