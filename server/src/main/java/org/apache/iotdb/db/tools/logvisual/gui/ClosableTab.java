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

import java.awt.Dimension;
import java.awt.event.ActionEvent;
import javax.swing.AbstractAction;
import javax.swing.JButton;
import javax.swing.JPanel;

abstract class ClosableTab extends JPanel {

  private JButton closeTabButton;

  ClosableTab(String name, TabCloseCallBack closeCallBack) {
    setName(name);
    setLayout(null);

    closeTabButton = new JButton("Close");
    closeTabButton.setLocation(720, 5);
    closeTabButton.setSize(new Dimension(70, 30));
    closeTabButton.setFont(closeTabButton.getFont().deriveFont(10.0f));
    add(closeTabButton);
    closeTabButton.addActionListener(new AbstractAction() {
      @Override
      public void actionPerformed(ActionEvent e) {
        closeCallBack.call(name);
      }
    });
  }

  public interface TabCloseCallBack {
    void call(String name);
  }
}