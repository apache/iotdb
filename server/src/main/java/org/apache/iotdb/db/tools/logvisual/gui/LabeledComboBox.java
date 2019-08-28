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
import java.util.Vector;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ComboBoxModel;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JLabel;

/**
 * LabeledComboBox is JComboBox with a label, using a map as the data source of the ComboBox.
 */
class LabeledComboBox<K, V> extends Box {

  private ComboBoxModel comboBoxModel;
  private JComboBox comboBox;

  LabeledComboBox(Map<K, V> itemMap, ComboSelectedCallback callback, String labelText) {
    super(BoxLayout.Y_AXIS);

    JLabel label = new JLabel(labelText);
    add(label);

    Vector vector = new Vector(itemMap.keySet());
    vector.sort(null);
    comboBoxModel = new DefaultComboBoxModel(vector);
    comboBox = new JComboBox(comboBoxModel);
    comboBox.setSelectedIndex(-1);

    add(comboBox);

    comboBox.addItemListener(e -> {
      K key = (K) e.getItem();
      callback.call(itemMap.get(key));
    });
  }

  public interface ComboSelectedCallback {
    // this methods accepts the value instead of the key in the itemMap
    void call(Object value);
  }
}