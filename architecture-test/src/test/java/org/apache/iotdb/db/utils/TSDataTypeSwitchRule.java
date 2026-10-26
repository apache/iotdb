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

package org.apache.iotdb.db.utils;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

final class TSDataTypeSwitchRule {
  private static final String SWITCH_MAP =
      "$SwitchMap$" + TSDataType.class.getName().replace('.', '$');
  private static final String SERVICE_DESCRIPTOR = Type.getDescriptor(TypeService.class);

  static final ArchRule RULE =
      classes()
          .should(
              new ArchCondition<>("only switch on TSDataType inside TypeService") {
                @Override
                public void check(JavaClass javaClass, ConditionEvents events) {
                  if (javaClass.getModifiers().contains(JavaModifier.SYNTHETIC)
                      || javaClass.isAssignableTo(TypeService.class)) {
                    return;
                  }
                  SwitchVisitor visitor = read(javaClass.getSource().orElseThrow().getUri());
                  for (SwitchAccess access : visitor.switches) {
                    if (!visitor.serviceMethods.contains(access.method)) {
                      events.add(
                          SimpleConditionEvent.violated(
                              javaClass,
                              javaClass.getName()
                                  + "."
                                  + access.method
                                  + " switches on TSDataType outside TypeService at ("
                                  + visitor.sourceFile
                                  + ":"
                                  + access.line
                                  + ")"));
                    }
                  }
                }
              })
          .allowEmptyShould(true);

  private TSDataTypeSwitchRule() {}

  static boolean hasSwitch(URI uri) {
    return !read(uri).switches.isEmpty();
  }

  private static SwitchVisitor read(URI uri) {
    try (InputStream input = uri.toURL().openStream()) {
      SwitchVisitor visitor = new SwitchVisitor();
      new ClassReader(input).accept(visitor, ClassReader.SKIP_FRAMES);
      return visitor;
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot inspect " + uri, e);
    }
  }

  // ArchUnit deliberately drops $SwitchMap$ accesses. Read them with ASM instead of treating
  // every ordinal() invocation as a switch, which would incorrectly reject ordinary enum usage.
  private static final class SwitchVisitor extends ClassVisitor {
    private final List<SwitchAccess> switches = new ArrayList<>();
    private final Set<String> serviceMethods = new HashSet<>();
    private String owner;
    private String sourceFile;
    private boolean synthetic;

    private SwitchVisitor() {
      super(Opcodes.ASM9);
    }

    @Override
    public void visit(
        int version,
        int access,
        String name,
        String signature,
        String superName,
        String[] interfaces) {
      owner = name;
      synthetic = (access & Opcodes.ACC_SYNTHETIC) != 0;
    }

    @Override
    public void visitSource(String source, String debug) {
      sourceFile = source;
    }

    @Override
    public MethodVisitor visitMethod(
        int access, String name, String descriptor, String signature, String[] exceptions) {
      if (synthetic) {
        // The compiler's mapping-array initializer is not a source-level switch.
        return null;
      }
      return new MethodVisitor(Opcodes.ASM9) {
        private int line;

        @Override
        public void visitLineNumber(int lineNumber, Label start) {
          line = lineNumber;
        }

        @Override
        public void visitFieldInsn(
            int opcode, String fieldOwner, String fieldName, String fieldType) {
          // The repository compiles with Java 17 javac, for both switch statements and expressions.
          if (opcode == Opcodes.GETSTATIC
              && fieldName.equals(SWITCH_MAP)
              && fieldType.equals("[I")) {
            switches.add(new SwitchAccess(name + descriptor, line));
          }
        }

        @Override
        public void visitInvokeDynamicInsn(
            String name, String descriptor, Handle bootstrap, Object... arguments) {
          // Only the lambda implementing TypeService is exempt, not all methods of its holder.
          if (bootstrap.getOwner().equals("java/lang/invoke/LambdaMetafactory")
              && Type.getReturnType(descriptor).getDescriptor().equals(SERVICE_DESCRIPTOR)
              && arguments.length > 1
              && arguments[1] instanceof Handle implementation
              && implementation.getOwner().equals(owner)
              && implementation.getName().startsWith("lambda$")) {
            serviceMethods.add(implementation.getName() + implementation.getDesc());
          }
        }
      };
    }
  }

  private static final class SwitchAccess {
    private final String method;
    private final int line;

    private SwitchAccess(String method, int line) {
      this.method = method;
      this.line = line;
    }
  }
}
