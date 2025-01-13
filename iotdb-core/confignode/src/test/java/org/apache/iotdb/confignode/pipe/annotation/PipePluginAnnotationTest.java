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

package org.apache.iotdb.confignode.pipe.annotation;

import org.apache.iotdb.commons.pipe.datastructure.visibility.Visibility;
import org.apache.iotdb.pipe.api.PipePlugin;

import org.junit.Test;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.commons.pipe.datastructure.visibility.VisibilityUtils.calculateFromPluginClass;
import static org.apache.iotdb.commons.pipe.datastructure.visibility.VisibilityUtils.isCompatible;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PipePluginAnnotationTest {

  @Test
  public void testPipePluginVisibility() {
    // Use the Reflections library to scan the classpath
    final Reflections reflections =
        new Reflections("org.apache.iotdb.confignode", new SubTypesScanner(false));
    final Set<Class<? extends PipePlugin>> subTypes = reflections.getSubTypesOf(PipePlugin.class);

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
    for (TreeNode node : root.children.values()) {
      node.visibility = Visibility.BOTH;
    }

    // Validate the correctness of the tree
    assertNotNull(root);

    // Validate the visibility compatibility of the tree
    assertTrue(validateTreeNode(root));
  }

  private static class TreeNode {
    Class<?> clazz;
    Visibility visibility;
    Map<Class<?>, TreeNode> children = new HashMap<>();

    TreeNode(final Class<?> clazz) {
      this.clazz = clazz;
      this.visibility = calculateFromPluginClass(clazz);
    }
  }

  // Get the super interface of an interface
  private Class<?> getSuperInterface(final Class<?> interfaceClass) {
    final Class<?>[] superInterfaces = interfaceClass.getInterfaces();
    return superInterfaces.length > 0 ? superInterfaces[0] : null;
  }

  private static boolean validateTreeNode(final TreeNode node) {
    for (final TreeNode child : node.children.values()) {
      if (!isCompatible(node.visibility, child.visibility)) {
        assertTrue(
            "Incompatible visibility detected:\n"
                + "Parent class: "
                + node.clazz.getName()
                + ", Visibility: "
                + node.visibility
                + "\n"
                + "Child class: "
                + child.clazz.getName()
                + ", Visibility: "
                + child.visibility,
            isCompatible(node.visibility, child.visibility));
      }
      if (!validateTreeNode(child)) {
        return false;
      }
    }
    return true;
  }
}
