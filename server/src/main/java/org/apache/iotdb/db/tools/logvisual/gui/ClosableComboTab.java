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

package org.apache.iotdb.db.tools.logvisual.gui;

import java.util.Map;

/**
 * ClosableComboTab is a ClosableTab with a comboBox.
 */
abstract class ClosableComboTab extends ClosableTab {

  private LabeledComboBox comboBox;

  ClosableComboTab(String name, Map comboItems, TabCloseCallBack tabCloseCallBack) {
    super(name, tabCloseCallBack);

    comboBox = new LabeledComboBox(comboItems, this::onItemSelected, "Please select an item to "
        + "view");
    comboBox.setLocation(0, 10);
    comboBox.setSize(650, 50);
    add(comboBox);
  }

  abstract void onItemSelected(Object object);
}