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

package org.apache.iotdb.commons.pipe.datastructure.visibility;

import org.apache.iotdb.commons.pipe.datastructure.result.Result;
import org.apache.iotdb.pipe.api.PipePlugin;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class VisibilityTestUtils {

  public static Result<Void, String> testVisibilityCompatibilityEntry(
      final Set<Class<? extends PipePlugin>> subTypes) {
    // Create root node
    final TreeNode root = new TreeNode(PipePlugin.class);

    // Build the tree
    final Map<Class<?>, TreeNode> nodeMap = new HashMap<>();
    nodeMap.put(PipePlugin.class, root);

    for (final Class<?> subType : subTypes) {
      final TreeNode node = new TreeNode(subType);
      nodeMap.put(subType, node);

      if (subType.isInterface()) {
        // Handle super interfaces of the interface
        final Class<?>[] superInterfaces = subType.getInterfaces();
        for (Class<?> superInterface : superInterfaces) {
          while (!nodeMap.containsKey(superInterface) && superInterface != null) {
            superInterface = getSuperInterface(superInterface);
          }

          if (superInterface != null) {
            nodeMap.get(superInterface).children.put(subType, node);
          }
        }
      } else {
        // Handle superclass of the class
        Class<?> superClass = subType.getSuperclass();
        if (superClass == Object.class) {
          // If the superclass is Object, check the implemented interfaces
          final Class<?>[] interfaces = subType.getInterfaces();
          for (final Class<?> iface : interfaces) {
            if (nodeMap.containsKey(iface)) {
              nodeMap.get(iface).children.put(subType, node);
            }
          }
        } else {
          while (!nodeMap.containsKey(superClass) && superClass != null) {
            superClass = superClass.getSuperclass();
          }

          if (superClass != null) {
            nodeMap.get(superClass).children.put(subType, node);
          }
        }
      }
    }

    root.visibility = Visibility.BOTH;
    for (final TreeNode node : root.children.values()) {
      node.visibility = Visibility.BOTH;
    }

    // Validate the visibility compatibility of the tree
    return validateTreeNode(root);
  }

  private static Result<Void, String> validateTreeNode(final TreeNode node) {
    for (final TreeNode child : node.children.values()) {
      if (!VisibilityUtils.isCompatible(node.visibility, child.visibility)) {
        return Result.err(
            String.format(
                "Incompatible visibility detected:\n"
                    + "Parent class: "
                    + node.clazz.getName()
                    + ", Visibility: "
                    + node.visibility
                    + "\n"
                    + "Child class: "
                    + child.clazz.getName()
                    + ", Visibility: "
                    + child.visibility));
      }
      final Result<Void, String> childResult = validateTreeNode(child);
      if (childResult.isErr()) {
        return childResult;
      }
    }
    return Result.ok(null);
  }

  // Get the super interface of an interface
  private static Class<?> getSuperInterface(final Class<?> interfaceClass) {
    final Class<?>[] superInterfaces = interfaceClass.getInterfaces();
    // TODO: We assume the first interface
    return superInterfaces.length > 0 ? superInterfaces[0] : null;
  }

  private static class TreeNode {
    Class<?> clazz;
    Visibility visibility;
    Map<Class<?>, TreeNode> children = new HashMap<>();

    TreeNode(final Class<?> clazz) {
      this.clazz = clazz;
      this.visibility = VisibilityUtils.calculateFromPluginClass(clazz);
    }
  }
}
