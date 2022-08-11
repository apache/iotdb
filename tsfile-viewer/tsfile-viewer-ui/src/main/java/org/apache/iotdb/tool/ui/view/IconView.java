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

package org.apache.iotdb.tool.ui.view;

import javafx.scene.image.Image;
import javafx.scene.image.ImageView;

/**
 * ImageView extension for icons.
 *
 * @author shenguanchu
 */
public class IconView extends ImageView {
  private static final int DEFAULT_ICON_SIZE = 16;

  public IconView() {}

  /** @param path Path to resource. */
  public IconView(String path) {
    this(new Image(path));
  }

  /** @param image Image resource. */
  public IconView(Image image) {
    super(image);
    fitHeightProperty().set(DEFAULT_ICON_SIZE);
    fitWidthProperty().set(DEFAULT_ICON_SIZE);
  }

  /**
   * @param image Image resource.
   * @param size Image width/height.
   */
  public IconView(Image image, int size) {
    super(image);
    fitHeightProperty().set(size);
    fitWidthProperty().set(size);
  }
}
