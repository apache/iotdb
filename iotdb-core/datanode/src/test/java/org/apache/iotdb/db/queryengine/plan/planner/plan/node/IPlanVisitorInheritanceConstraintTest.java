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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class IPlanVisitorInheritanceConstraintTest {

  private static final Pattern TYPE_DECLARATION_PATTERN =
      Pattern.compile(
          "(class|interface)\\s+(\\w+)\\s*(?:<[^>{;]*>)?\\s*"
              + "(?:extends\\s+([^{]+?))?\\s*"
              + "(?:implements\\s+([^{]+?))?\\s*\\{",
          Pattern.DOTALL);

  private static final Pattern TYPE_REFERENCE_PATTERN = Pattern.compile("\\b([A-Z]\\w*)\\b");

  @Test
  public void testIPlanVisitorSubtypesAlsoInheritICoreQueryPlanVisitor() throws IOException {
    final Path moduleBaseDir = resolveModuleBaseDir();
    final Path projectRoot = moduleBaseDir.getParent().getParent();
    final Path iotdbCoreDir = projectRoot.resolve("iotdb-core");

    final Map<String, List<TypeDefinition>> typeDefinitions = new HashMap<>();
    try (Stream<Path> paths = Files.walk(iotdbCoreDir)) {
      paths
          .filter(path -> path.toString().endsWith(".java"))
          .filter(path -> path.toString().contains("/src/main/java/"))
          .forEach(path -> collectTypeDefinitions(iotdbCoreDir, path, typeDefinitions));
    }

    final List<String> violations = new ArrayList<>();
    for (List<TypeDefinition> definitions : typeDefinitions.values()) {
      for (TypeDefinition definition : definitions) {
        if (!definition.typeName.equals("IPlanVisitor")
            && !definition.typeName.equals("ICoreQueryPlanVisitor")
            && isSubtypeOf(definition, "IPlanVisitor", typeDefinitions, new ArrayDeque<>())
            && !isSubtypeOf(
                definition, "ICoreQueryPlanVisitor", typeDefinitions, new ArrayDeque<>())) {
          violations.add(definition.location);
        }
      }
    }

    Collections.sort(violations);
    Assert.assertTrue(
        "Found IPlanVisitor subtypes that do not also inherit ICoreQueryPlanVisitor: "
            + violations
            + ". This constraint may be broken intentionally, but anyone doing so must update "
            + "the fragile PlanNode.accept(...) contract and this test together.",
        violations.isEmpty());
  }

  private static Path resolveModuleBaseDir() {
    final String basedir = System.getProperty("basedir");
    if (basedir != null) {
      return Paths.get(basedir);
    }

    Path current = Paths.get("").toAbsolutePath();
    while (current != null) {
      if (Files.exists(current.resolve("pom.xml"))
          && current.endsWith(Paths.get("iotdb-core", "datanode"))) {
        return current;
      }
      current = current.getParent();
    }

    throw new IllegalStateException(
        "Cannot resolve datanode module base directory. "
            + "Please run the test from the iotdb project workspace.");
  }

  private static void collectTypeDefinitions(
      final Path scanRoot,
      final Path javaFile,
      final Map<String, List<TypeDefinition>> typeDefinitions) {
    final String sanitizedContent;
    try {
      sanitizedContent = sanitize(new String(Files.readAllBytes(javaFile), StandardCharsets.UTF_8));
    } catch (final IOException e) {
      throw new RuntimeException("Failed to read " + javaFile, e);
    }

    final Matcher matcher = TYPE_DECLARATION_PATTERN.matcher(sanitizedContent);
    while (matcher.find()) {
      final String typeName = matcher.group(2);
      final String inheritanceClause =
          (matcher.group(3) == null ? "" : matcher.group(3))
              + " "
              + (matcher.group(4) == null ? "" : matcher.group(4));
      typeDefinitions
          .computeIfAbsent(typeName, key -> new ArrayList<>())
          .add(
              new TypeDefinition(
                  typeName, inheritanceClause, scanRoot.relativize(javaFile) + "#" + typeName));
    }
  }

  private static boolean isSubtypeOf(
      final TypeDefinition definition,
      final String targetTypeName,
      final Map<String, List<TypeDefinition>> typeDefinitions,
      final Deque<String> path) {
    if (definition.typeName.equals(targetTypeName)
        || mentionsType(definition.inheritanceClause, targetTypeName)) {
      return true;
    }

    if (path.contains(definition.location)) {
      return false;
    }
    path.push(definition.location);

    try {
      for (String referencedType : extractReferencedTypes(definition.inheritanceClause)) {
        if (referencedType.equals(definition.typeName)) {
          continue;
        }
        for (TypeDefinition referencedDefinition :
            typeDefinitions.getOrDefault(referencedType, Collections.emptyList())) {
          if (isSubtypeOf(referencedDefinition, targetTypeName, typeDefinitions, path)) {
            return true;
          }
        }
      }
      return false;
    } finally {
      path.pop();
    }
  }

  private static Set<String> extractReferencedTypes(final String inheritanceClause) {
    final Set<String> referencedTypes = new HashSet<>();
    final Matcher matcher = TYPE_REFERENCE_PATTERN.matcher(inheritanceClause);
    while (matcher.find()) {
      referencedTypes.add(matcher.group(1));
    }
    return referencedTypes;
  }

  private static boolean mentionsType(final String clause, final String typeName) {
    return Pattern.compile("(^|[^\\w$])" + Pattern.quote(typeName) + "([^\\w$]|$)")
        .matcher(clause)
        .find();
  }

  private static String sanitize(final String content) {
    final StringBuilder builder = new StringBuilder(content.length());
    final int normal = 0;
    final int lineComment = 1;
    final int blockComment = 2;
    final int doubleQuotedString = 3;
    final int singleQuotedString = 4;

    int state = normal;
    for (int i = 0; i < content.length(); i++) {
      final char current = content.charAt(i);
      final char next = i + 1 < content.length() ? content.charAt(i + 1) : '\0';

      switch (state) {
        case normal:
          if (current == '/' && next == '/') {
            builder.append(' ').append(' ');
            state = lineComment;
            i++;
          } else if (current == '/' && next == '*') {
            builder.append(' ').append(' ');
            state = blockComment;
            i++;
          } else if (current == '"') {
            builder.append('"').append('"');
            state = doubleQuotedString;
          } else if (current == '\'') {
            builder.append('\'').append('\'');
            state = singleQuotedString;
          } else {
            builder.append(current);
          }
          break;
        case lineComment:
          if (current == '\n' || current == '\r') {
            builder.append(current);
            state = normal;
          } else {
            builder.append(' ');
          }
          break;
        case blockComment:
          if (current == '*' && next == '/') {
            builder.append(' ').append(' ');
            state = normal;
            i++;
          } else if (current == '\n' || current == '\r') {
            builder.append(current);
          } else {
            builder.append(' ');
          }
          break;
        case doubleQuotedString:
          if (current == '\\' && i + 1 < content.length()) {
            builder.append(' ').append(' ');
            i++;
          } else if (current == '"') {
            builder.append('"');
            state = normal;
          } else if (current == '\n' || current == '\r') {
            builder.append(current);
            state = normal;
          } else {
            builder.append(' ');
          }
          break;
        case singleQuotedString:
          if (current == '\\' && i + 1 < content.length()) {
            builder.append(' ').append(' ');
            i++;
          } else if (current == '\'') {
            builder.append('\'');
            state = normal;
          } else if (current == '\n' || current == '\r') {
            builder.append(current);
            state = normal;
          } else {
            builder.append(' ');
          }
          break;
        default:
          throw new IllegalStateException("Unknown sanitize state: " + state);
      }
    }
    return builder.toString();
  }

  private static final class TypeDefinition {

    private final String typeName;
    private final String inheritanceClause;
    private final String location;

    private TypeDefinition(
        final String typeName, final String inheritanceClause, final String location) {
      this.typeName = typeName;
      this.inheritanceClause = inheritanceClause;
      this.location = location;
    }
  }
}
