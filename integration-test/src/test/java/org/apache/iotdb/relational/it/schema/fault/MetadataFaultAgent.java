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

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Launch-time instrumentation for metadata IT child processes only. No production classes are
 * replaced on disk. The gates run before the receiver takes its locks, or after its finally block.
 * The control channel only arms gates and reads state; it never edits product caches or versions.
 */
public final class MetadataFaultAgent {
  public static final String INVALIDATE = "invalidateColumnCache";
  public static final String UPDATE = "updateAttribute";
  public static final String FILL = "putAttributes";
  private static final String RPC =
      "org/apache/iotdb/db/protocol/thrift/impl/DataNodeInternalRPCServiceImpl";
  private static final String FETCHER =
      "org/apache/iotdb/db/queryengine/plan/relational/metadata/fetcher/";
  private static final String UPDATER =
      "org/apache/iotdb/db/schemaengine/schemaregion/attribute/update/";
  private static final String LEASE = "org/apache/iotdb/db/schemaengine/lease/MetadataLeaseManager";
  private static final Map<String, Rule> RULES = new ConcurrentHashMap<>();
  private static final Map<String, AtomicLong> COUNTS = new ConcurrentHashMap<>();
  private static final Set<Object> UPDATERS = ConcurrentHashMap.newKeySet();

  private MetadataFaultAgent() {}

  public static void premain(String directory, Instrumentation instrumentation) throws IOException {
    instrumentation.addTransformer(new Transformer());
    ServerSocket server = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
    Thread controller = new Thread(() -> serve(server), "metadata-it-fault-controller");
    controller.setDaemon(true);
    controller.start();
    Path endpoint = Paths.get(directory, ProcessHandle.current().pid() + ".port");
    Files.write(endpoint, Collections.singletonList(Integer.toString(server.getLocalPort())));
  }

  private static void serve(ServerSocket server) {
    while (!server.isClosed()) {
      try (Socket socket = server.accept();
          DataInputStream input = new DataInputStream(socket.getInputStream());
          DataOutputStream output = new DataOutputStream(socket.getOutputStream())) {
        socket.setSoTimeout(5_000);
        Map<String, String> response;
        try {
          String command = input.readUTF();
          String hook = input.readUTF();
          String mode = input.readUTF();
          String match = input.readUTF();
          String attributes = input.readUTF();
          response = command(command, hook, mode, match, attributes);
        } catch (Throwable e) {
          response = Collections.singletonMap("error", e.toString());
        }
        output.writeInt(response.size());
        for (Map.Entry<String, String> entry : response.entrySet()) {
          output.writeUTF(entry.getKey());
          output.writeUTF(entry.getValue());
        }
      } catch (IOException e) {
        // A failed control connection must not disable subsequent release/cleanup requests.
        System.err.println("Metadata IT control connection failed: " + e);
      }
    }
  }

  private static Map<String, String> command(
      String command, String hook, String mode, String match, String attributes) throws Exception {
    switch (command) {
      case "arm":
        Rule old = RULES.putIfAbsent(hook, new Rule(mode, match, attributes));
        if (old != null) {
          throw new IllegalStateException("Gate already armed: " + hook);
        }
        return Collections.emptyMap();
      case "status":
        Rule rule = RULES.get(hook);
        return rule == null ? Collections.emptyMap() : rule.snapshot();
      case "release":
        Rule toRelease = RULES.get(hook);
        if (toRelease != null) {
          toRelease.release.countDown();
        }
        return Collections.emptyMap();
      case "reset":
        RULES.values().forEach(value -> value.release.countDown());
        RULES.clear();
        return Collections.emptyMap();
      case "lease":
        Object lease = singleton(LEASE.replace('/', '.'));
        Map<String, String> state = new HashMap<>();
        state.put("heartbeats", Long.toString(count("triggerCheckWithHeartBeat:exit")));
        state.put("clears", Long.toString(count("clearCacheAndPullMetaData:enter")));
        state.put("fenced", invoke(lease, "isFenced").toString());
        state.put("elapsed", invoke(lease, "getMillisSinceLastConfigNodeHeartbeat").toString());
        state.put("threshold", field(lease, "fenceThresholdMs").toString());
        return state;
      case "updaters":
        return updaterState(match);
      case "count":
        return Collections.singletonMap("count", Long.toString(count(hook)));
      default:
        throw new IllegalArgumentException(command);
    }
  }

  private static long count(String hook) {
    AtomicLong count = COUNTS.get(hook);
    return count == null ? 0 : count.get();
  }

  private static Map<String, String> updaterState(String database) throws Exception {
    Map<String, String> state = new HashMap<>();
    int pending = 0;
    int detail = 0;
    int clear = 0;
    int found = 0;
    for (Object updater : UPDATERS) {
      if (!field(updater, "databaseName").toString().equals(database)) {
        continue;
      }
      found++;
      Map<?, ?> containers = (Map<?, ?>) field(updater, "attributeUpdateMap");
      pending += containers.size();
      for (Object container : containers.values()) {
        if (container.getClass().getSimpleName().equals("UpdateDetailContainer")) {
          detail++;
        } else {
          clear++;
        }
      }
      Object statistics = field(field(updater, "regionStatistics"), "schemaEngineStatistics");
      state.put("usage", invoke(statistics, "getMemoryUsage").toString());
      state.put("capacity", invoke(statistics, "getMemoryCapacity").toString());
      state.put("allowCreate", invoke(statistics, "isAllowToCreateNewSeries").toString());
    }
    state.put("found", Integer.toString(found));
    state.put("pending", Integer.toString(pending));
    state.put("detail", Integer.toString(detail));
    state.put("clear", Integer.toString(clear));
    state.put("degrades", Long.toString(count("degrade:exit")));
    return state;
  }

  private static Object singleton(String name) throws Exception {
    return Class.forName(name).getMethod("getInstance").invoke(null);
  }

  private static Object invoke(Object object, String method) throws Exception {
    return object.getClass().getMethod(method).invoke(object);
  }

  private static Object field(Object object, String name) throws Exception {
    Class<?> type = object.getClass();
    while (type != null) {
      try {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(object);
      } catch (NoSuchFieldException e) {
        type = type.getSuperclass();
      }
    }
    throw new NoSuchFieldException(name);
  }

  /** Called only by the instrumented child process, never by the test's control connection. */
  public static void hit(String hook, Object receiver, Object[] arguments, Object result)
      throws Throwable {
    COUNTS.computeIfAbsent(hook, key -> new AtomicLong()).incrementAndGet();
    if (receiver.getClass().getSimpleName().equals("DeviceAttributeCacheUpdater")) {
      UPDATERS.add(receiver);
    }
    if (hook.endsWith(":exit") && arguments.length > 0) {
      Rule entryRule = RULES.get(hook.substring(0, hook.length() - 5) + ":enter");
      if (entryRule != null && arguments[0] == entryRule.firstArgument) {
        entryRule.completed.incrementAndGet();
      }
    }
    Rule rule = RULES.get(hook);
    if (rule == null) {
      return;
    }
    String description = Arrays.toString(arguments);
    if (receiver.getClass().getSimpleName().equals("TableDeviceSchemaCache")
        && arguments.length > 2
        && arguments[2] instanceof Map) {
      StringBuilder values = new StringBuilder();
      ((Map<?, ?>) arguments[2])
          .forEach(
              (key, value) ->
                  values.append(key).append('=').append(binaryString(value)).append(';'));
      description += " fetchedAttributes=" + values;
    }
    if (receiver.getClass().getSimpleName().equals("GeneralRegionAttributeSecurityService")) {
      Map<?, ?> databases = (Map<?, ?>) field(receiver, "regionId2DatabaseMap");
      for (Object region : ((Map<?, ?>) arguments[0]).keySet()) {
        description += " database=" + databases.get(region);
      }
    }
    if (!description.contains(rule.match)) {
      return;
    }
    String attributes = batchAttributes(arguments);
    if (!attributes.contains(rule.expectedAttributes)) {
      return;
    }
    final long occurrence;
    // Publish the first batch's identity before a concurrent retry can decide whether to pass.
    // Do not retain this monitor while waiting at the gate.
    synchronized (rule) {
      occurrence = rule.matches.incrementAndGet();
      if (occurrence == 1) {
        rule.description = description.substring(0, Math.min(description.length(), 4_096));
        rule.stack = Arrays.toString(Thread.currentThread().getStackTrace());
        rule.version = version(arguments);
        rule.batchType = batchType(arguments);
        rule.attributes = attributes;
        rule.firstArgument = arguments.length == 0 ? null : arguments[0];
        rule.captured = true;
      }
    }
    for (Object argument : arguments) {
      if (argument != null && argument.getClass().getSimpleName().equals("UpdateClearContainer")) {
        rule.clear.incrementAndGet();
      }
    }
    if (rule.release.getCount() == 0) {
      rule.passed.incrementAndGet();
      return;
    }
    if (rule.mode.equals("HOLD_FIRST") && occurrence == 1) {
      rule.held.countDown();
      if (!rule.release.await(120, TimeUnit.SECONDS)) {
        throw new AssertionError("Metadata IT gate was not released: " + hook);
      }
      rule.finished.incrementAndGet();
      return;
    }
    if (rule.mode.equals("DROP")
        || (rule.mode.equals("HOLD_FIRST")
            && rule.version != null
            && rule.version.equals(version(arguments)))) {
      rule.dropped.incrementAndGet();
      // Thrift's processor rethrows TTransportException and closes this connection. In particular,
      // this does not manufacture an application error status that would bypass transport retries.
      throw (Throwable)
          Class.forName("org.apache.thrift.transport.TTransportException")
              .getConstructor(String.class)
              .newInstance("Metadata IT discarded " + hook);
    }
    rule.passed.incrementAndGet();
  }

  private static String version(Object[] arguments) throws Exception {
    if (arguments.length == 0) {
      return null;
    }
    if (arguments[0] instanceof Map) {
      StringBuilder versions = new StringBuilder();
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) arguments[0]).entrySet()) {
        versions.append(entry.getKey()).append('=').append(invoke(entry.getValue(), "getLeft"));
      }
      return versions.toString();
    }
    if (!arguments[0].getClass().getSimpleName().equals("TAttributeUpdateReq")) {
      return null;
    }
    Map<?, ?> updates = (Map<?, ?>) invoke(arguments[0], "getAttributeUpdateMap");
    StringBuilder versions = new StringBuilder();
    for (Map.Entry<?, ?> entry : updates.entrySet()) {
      versions.append(entry.getKey()).append('=').append(invoke(entry.getValue(), "getVersion"));
    }
    return versions.toString();
  }

  private static String binaryString(Object value) {
    if (value == null) {
      return "null";
    }
    try {
      for (Method method : value.getClass().getMethods()) {
        if (method.getName().equals("getStringValue")
            && method.getParameterCount() == 1
            && method.getParameterTypes()[0].getName().equals("java.nio.charset.Charset")) {
          return method.invoke(value, java.nio.charset.StandardCharsets.UTF_8).toString();
        }
      }
    } catch (ReflectiveOperationException ignored) {
      // Fall back to the value's diagnostic form.
    }
    return String.valueOf(value);
  }

  private static String batchType(Object[] arguments) throws Exception {
    for (byte[] body : batchBodies(arguments)) {
      return body[0] == 0 ? "clear" : "detail";
    }
    return "empty";
  }

  private static String batchAttributes(Object[] arguments) throws Exception {
    List<byte[]> bodies = batchBodies(arguments);
    if (bodies.isEmpty()) {
      return "";
    }
    Method decode =
        Class.forName((UPDATER + "DeviceAttributeCacheUpdater").replace('/', '.'))
            .getMethod("getContainer", byte[].class);
    StringBuilder values = new StringBuilder();
    for (byte[] body : bodies) {
      // Deserialize a separate diagnostic copy; never alter the request or product cache.
      Object container = decode.invoke(null, body);
      if (!container.getClass().getSimpleName().equals("UpdateDetailContainer")) {
        continue;
      }
      for (Map.Entry<?, ?> table : ((Map<?, ?>) invoke(container, "getUpdateMap")).entrySet()) {
        for (Map.Entry<?, ?> device : ((Map<?, ?>) table.getValue()).entrySet()) {
          for (Map.Entry<?, ?> attribute : ((Map<?, ?>) device.getValue()).entrySet()) {
            values
                .append(table.getKey())
                .append('/')
                .append(device.getKey())
                .append(':')
                .append(attribute.getKey())
                .append('=')
                .append(binaryString(attribute.getValue()))
                .append(';');
          }
        }
      }
    }
    return values.substring(0, Math.min(values.length(), 4_096));
  }

  private static List<byte[]> batchBodies(Object[] arguments) throws Exception {
    List<byte[]> bodies = new ArrayList<>();
    if (arguments.length == 0) {
      return bodies;
    }
    if (arguments[0].getClass().getSimpleName().equals("TAttributeUpdateReq")) {
      for (Object update : ((Map<?, ?>) invoke(arguments[0], "getAttributeUpdateMap")).values()) {
        bodies.add((byte[]) invoke(update, "getBody"));
      }
    } else if (arguments[0] instanceof Map) {
      // At sendUpdateRequestAndMayShrink's exit this map contains only acknowledged batches.
      for (Object pair : ((Map<?, ?>) arguments[0]).values()) {
        for (Object body : ((Map<?, ?>) invoke(pair, "getRight")).values()) {
          bodies.add((byte[]) body);
        }
      }
    }
    return bodies;
  }

  private static final class Rule {
    private final String mode;
    private final String match;
    private final String expectedAttributes;
    private final AtomicLong matches = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong passed = new AtomicLong();
    private final AtomicLong finished = new AtomicLong();
    private final AtomicLong clear = new AtomicLong();
    private final AtomicLong completed = new AtomicLong();
    private final CountDownLatch held = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private volatile String description = "";
    private volatile String stack = "";
    private volatile String version;
    private volatile String batchType = "";
    private volatile String attributes = "";
    private volatile Object firstArgument;
    private volatile boolean captured;

    private Rule(String mode, String match, String expectedAttributes) {
      this.mode = mode;
      this.match = match;
      this.expectedAttributes = expectedAttributes;
    }

    private Map<String, String> snapshot() {
      Map<String, String> state = new HashMap<>();
      state.put("matches", Long.toString(matches.get()));
      state.put("dropped", Long.toString(dropped.get()));
      state.put("passed", Long.toString(passed.get()));
      state.put("finished", Long.toString(finished.get()));
      state.put("held", Boolean.toString(held.getCount() == 0));
      state.put("description", description);
      state.put("stack", stack);
      state.put("version", String.valueOf(version));
      state.put("batchType", batchType);
      state.put("attributes", attributes);
      state.put("clear", Long.toString(clear.get()));
      state.put("completed", Long.toString(completed.get()));
      state.put("captured", Boolean.toString(captured));
      return state;
    }
  }

  private static boolean selected(String owner, String method, boolean exit) {
    if (owner.equals(RPC)) {
      return method.equals(INVALIDATE)
          || method.equals(UPDATE)
          || method.equals("deleteColumnData");
    }
    if (owner.equals(FETCHER + "cache/TableDeviceSchemaCache")) {
      return method.equals(FILL) && !exit;
    }
    if (owner.equals(FETCHER + "TableDeviceCacheAttributeGuard")) {
      return (method.equals("handleAttributeUpdate") && exit)
          || (method.equals("handleContainer") && !exit);
    }
    if (owner.equals(LEASE)) {
      return (method.equals("triggerCheckWithHeartBeat") && exit)
          || (method.equals("clearCacheAndPullMetaData") && !exit);
    }
    if (owner.equals(UPDATER + "DeviceAttributeCacheUpdater")) {
      return exit && (method.equals("afterUpdate") || method.equals("commit"));
    }
    if (owner.equals(UPDATER + "GeneralRegionAttributeSecurityService")) {
      return method.equals("sendUpdateRequestAndMayShrink") && exit;
    }
    return owner.equals(UPDATER + "UpdateDetailContainer") && method.equals("degrade") && exit;
  }

  private static final class Transformer implements ClassFileTransformer {
    @Override
    public byte[] transform(
        ClassLoader loader,
        String name,
        Class<?> redefining,
        ProtectionDomain domain,
        byte[] bytes) {
      if (!(name.equals(RPC)
          || name.startsWith(FETCHER)
          || name.startsWith(UPDATER)
          || name.equals(LEASE))) {
        return null;
      }
      ClassReader reader = new ClassReader(bytes);
      // javac can discard this/parameter locals from a return frame when the original code no
      // longer reads them. Exit hooks do read them, so retaining the original frames is invalid.
      ClassWriter writer = new LoaderClassWriter(reader, loader);
      reader.accept(new HookVisitor(writer, name), ClassReader.SKIP_FRAMES);
      return writer.toByteArray();
    }
  }

  private static final class LoaderClassWriter extends ClassWriter {
    private final ClassLoader loader;

    private LoaderClassWriter(ClassReader reader, ClassLoader loader) {
      super(reader, ClassWriter.COMPUTE_FRAMES);
      this.loader = loader;
    }

    @Override
    protected ClassLoader getClassLoader() {
      return loader;
    }
  }

  private static final class HookVisitor extends ClassVisitor {
    private final String owner;

    private HookVisitor(ClassVisitor visitor, String owner) {
      super(Opcodes.ASM9, visitor);
      this.owner = owner;
    }

    @Override
    public MethodVisitor visitMethod(
        int access, String name, String descriptor, String signature, String[] exceptions) {
      MethodVisitor visitor = super.visitMethod(access, name, descriptor, signature, exceptions);
      if (!selected(owner, name, false) && !selected(owner, name, true)) {
        return visitor;
      }
      return new HookMethodVisitor(visitor, owner, name, descriptor);
    }
  }

  private static final class HookMethodVisitor extends MethodVisitor {
    private final String owner;
    private final String method;
    private final String descriptor;

    private HookMethodVisitor(
        MethodVisitor visitor, String owner, String method, String descriptor) {
      super(Opcodes.ASM9, visitor);
      this.owner = owner;
      this.method = method;
      this.descriptor = descriptor;
    }

    @Override
    public void visitCode() {
      super.visitCode();
      if (selected(owner, method, false)) {
        hook(false, false);
      }
    }

    @Override
    public void visitInsn(int opcode) {
      if ((opcode == Opcodes.RETURN || opcode == Opcodes.ARETURN)
          && selected(owner, method, true)) {
        hook(true, opcode == Opcodes.ARETURN);
      }
      super.visitInsn(opcode);
    }

    private void hook(boolean exit, boolean objectReturn) {
      if (objectReturn) {
        super.visitInsn(Opcodes.DUP);
      }
      super.visitLdcInsn(method + (exit ? ":exit" : ":enter"));
      if (objectReturn) {
        super.visitInsn(Opcodes.SWAP);
      }
      super.visitVarInsn(Opcodes.ALOAD, 0);
      if (objectReturn) {
        super.visitInsn(Opcodes.SWAP);
      }
      Type[] arguments = Type.getArgumentTypes(descriptor);
      super.visitLdcInsn(arguments.length);
      super.visitTypeInsn(Opcodes.ANEWARRAY, "java/lang/Object");
      int local = 1;
      for (int i = 0; i < arguments.length; i++) {
        super.visitInsn(Opcodes.DUP);
        super.visitLdcInsn(i);
        // Selected entry/exit methods have reference parameters only.
        if (arguments[i].getSort() != Type.OBJECT && arguments[i].getSort() != Type.ARRAY) {
          throw new IllegalStateException("Unexpected hook signature: " + descriptor);
        }
        super.visitVarInsn(Opcodes.ALOAD, local++);
        super.visitInsn(Opcodes.AASTORE);
      }
      if (objectReturn) {
        super.visitInsn(Opcodes.SWAP);
      } else {
        super.visitInsn(Opcodes.ACONST_NULL);
      }
      super.visitMethodInsn(
          Opcodes.INVOKESTATIC,
          Type.getInternalName(MetadataFaultAgent.class),
          "hit",
          "(Ljava/lang/String;Ljava/lang/Object;[Ljava/lang/Object;Ljava/lang/Object;)V",
          false);
    }
  }
}
