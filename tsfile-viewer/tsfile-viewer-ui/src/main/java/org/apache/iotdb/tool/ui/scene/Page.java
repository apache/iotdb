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

package org.apache.iotdb.tool.ui.scene;

import javafx.scene.Parent;
import javafx.scene.Scene;

/**
 * 使用page替换scene概念
 *
 * @author oortCloudFei
 */
public abstract class Page<P extends Parent> {

  protected P root;

  private Scene scene;

  private double width;

  private double height;

  public Page(P root, double width, double height) {
    this.root = root;
    this.width = width;
    this.height = height;
    this.scene = new Scene(root, width, height);
  }

  public String getName() {
    return this.getClass().getSimpleName();
  }

  public double getWidth() {
    return width;
  }

  public double getHeight() {
    return height;
  }

  public Scene getScene() {
    return scene;
  }

  public P getRoot() {
    return root;
  }
}
