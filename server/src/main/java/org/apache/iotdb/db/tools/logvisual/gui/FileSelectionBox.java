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

import java.awt.event.ActionEvent;
import java.io.File;
import javax.swing.AbstractAction;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JTextField;

/**
 * FileSelectionBox is a combination of a JLabel (showing the name of this component), a
 * JTextField (showing the path of the chosen file) and a JButton (on clicking it the user can
 * choose a single file).
 */
class FileSelectionBox extends Box{

  private JLabel panelName;
  private JTextField filePathField;
  private JButton selectFileButton;
  private FilePathBoxSelectionCallBack callBack;

  FileSelectionBox(String name, FilePathBoxSelectionCallBack callBack, String
      defaultFilePath) {
    super(BoxLayout.X_AXIS);
    this.callBack = callBack;

    panelName = new JLabel(name);
    filePathField = new JTextField("No file is selected");

    filePathField.setEditable(false);
    selectFileButton = new JButton("Select");
    selectFileButton.addActionListener(new AbstractAction() {
      @Override
      public void actionPerformed(ActionEvent e) {
        onSelectFileButtonClick();
      }
    });

    Box vBox = Box.createVerticalBox();
    vBox.add(panelName);
    vBox.add(filePathField);

    add(vBox);
    add(Box.createHorizontalStrut(10));
    add(selectFileButton);

    if (defaultFilePath != null) {
      // select the default file if provided
      File defaultFile = new File(defaultFilePath);
      if (!defaultFile.exists()) {
        JOptionPane.showMessageDialog(this, panelName.getText() + ":default file " +
            defaultFilePath + " does not exist");
      } else {
        filePathField.setText(defaultFilePath);
        callBack.call(defaultFile);
      }
    }
  }

  private void onSelectFileButtonClick() {
    JFileChooser fileChooser = new JFileChooser();
    File currentFile = new File(filePathField.getText());
    if (currentFile.exists()) {
      if (currentFile.isDirectory()) {
        fileChooser.setCurrentDirectory(currentFile);
      } else {
        fileChooser.setCurrentDirectory(currentFile.getParentFile());
      }
    }

    int status = fileChooser.showOpenDialog(this);
    if (status == JFileChooser.APPROVE_OPTION) {
      // only one file is allowed
      File chosenFile = fileChooser.getSelectedFile();
      callBack.call(chosenFile);
      filePathField.setText(chosenFile.getPath());
    }
  }

  interface FilePathBoxSelectionCallBack {
    void call(File chosenFile);
  }
}