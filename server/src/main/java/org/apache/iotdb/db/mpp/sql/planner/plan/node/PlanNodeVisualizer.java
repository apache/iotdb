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

package org.apache.iotdb.db.mpp.sql.planner.plan.node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PlanNodeVisualizer {
  private static final String INDENT = " ";
  private static final String HENG = "─";
  private static final String SHU = "│";
  private static final String LEFT_BOTTOM = "└";
  private static final String RIGHT_BOTTOM = "┘";
  private static final String LEFT_TOP = "┌";
  private static final String RIGHT_TOP = "┐";
  private static final String SHANG = "┴";
  private static final String XIA = "┬";
  private static final String CROSS = "┼";

  private static final int BOX_MARGIN = 1;
  private static final int CONNECTION_LINE_HEIGHT = 2;

  private static class Box {
    private PlanNode node;
    private List<Box> children;
    private int boxWidth;
    private int lineWidth;
    private List<String> lines;
    private int startPosition;
    private int endPosition;
    private int midPosition;

    public Box(PlanNode node) {
      this.node = node;
      this.boxWidth = getBoxWidth();
      this.children = new ArrayList<>();
      this.lines = new ArrayList<>();
    }

    public int getBoxWidth() {
      List<String> boxLines = node.getBoxString();
      int width = 0;
      for (String line : boxLines) {
        width = Math.max(width, line.length());
      }
      return width + 2;
    }

    public String getLine(int idx) {
      if (idx < lines.size()) {
        return lines.get(idx);
      }
      return genEmptyLine(lineWidth);
    }

    private String genEmptyLine(int lineWidth) {
      StringBuilder line = new StringBuilder();
      for (int i = 0; i < lineWidth; i++) {
        line.append(INDENT);
      }
      return line.toString();
    }

    public int getChildrenLineCount() {
      int count = 0;
      for (Box child : children) {
        count = Math.max(count, child.lines.size());
      }
      return count;
    }

    public int childCount() {
      return children.size();
    }

    public Box getChild(int idx) {
      return children.get(idx);
    }
  }

  public static List<String> toBoxLines(PlanNode root) {
    Box box = buildBoxTree(root);
    calculateBoxParams(box);
    buildBoxLines(box);
    return box.lines;
  }

  public static void printAsBox(PlanNode root) {
    for (String line : toBoxLines(root)) {
      System.out.println(line);
    }
  }

  private static Box buildBoxTree(PlanNode root) {
    Box box = new Box(root);
    for (PlanNode child : root.getChildren()) {
      box.children.add(buildBoxTree(child));
    }
    return box;
  }

  private static void calculateBoxParams(Box box) {
    int childrenWidth = 0;
    for (Box child : box.children) {
      calculateBoxParams(child);
      childrenWidth += child.lineWidth;
    }
    childrenWidth += box.childCount() > 1 ? (box.childCount() - 1) * BOX_MARGIN : 0;
    box.lineWidth = Math.max(box.boxWidth, childrenWidth);
    box.startPosition = (box.lineWidth - box.boxWidth) / 2;
    box.endPosition = box.startPosition + box.boxWidth - 1;
    box.midPosition = box.lineWidth / 2;
  }

  private static void buildBoxLines(Box box) {
    box.lines.add(printBoxEdge(box, true));
    // Print value
    for (String valueLine : box.node.getBoxString()) {
      StringBuilder line = new StringBuilder();
      for (int i = 0; i < box.lineWidth; i++) {
        if (i < box.startPosition) {
          line.append(INDENT);
          continue;
        }
        if (i > box.endPosition) {
          line.append(INDENT);
          continue;
        }
        if (i == box.startPosition || i == box.endPosition) {
          line.append(SHU);
          continue;
        }
        if (i - box.startPosition - 1 < valueLine.length()) {
          line.append(valueLine.charAt(i - box.startPosition - 1));
        } else {
          line.append(INDENT);
        }
      }
      box.lines.add(line.toString());
    }
    box.lines.add(printBoxEdge(box, false));

    // No child, return
    if (box.children.size() == 0) {
      return;
    }

    // Print Connection Line
    if (box.children.size() == 1) {
      for (int i = 0; i < CONNECTION_LINE_HEIGHT; i++) {
        StringBuilder line = new StringBuilder();
        for (int j = 0; j < box.lineWidth; j++) {
          line.append(j == box.midPosition ? SHU : INDENT);
        }
        box.lines.add(line.toString());
      }
    } else {
      Map<Integer, String> symbolMap = new HashMap<>();
      Map<Integer, Boolean> childMidPositionMap = new HashMap<>();
      symbolMap.put(box.midPosition, SHANG);
      for (int i = 0; i < box.children.size(); i++) {
        int childMidPosition = getChildMidPosition(box, i);
        childMidPositionMap.put(childMidPosition, true);
        if (childMidPosition == box.midPosition) {
          symbolMap.put(box.midPosition, CROSS);
          continue;
        }
        symbolMap.put(
            childMidPosition, i == 0 ? LEFT_TOP : i == box.children.size() - 1 ? RIGHT_TOP : XIA);
      }
      StringBuilder line1 = new StringBuilder();
      for (int i = 0; i < box.lineWidth; i++) {
        if (i < getChildMidPosition(box, 0) || i > getChildMidPosition(box, box.childCount() - 1)) {
          line1.append(INDENT);
          continue;
        }
        line1.append(symbolMap.getOrDefault(i, HENG));
      }
      box.lines.add(line1.toString());

      for (int row = 1; row < CONNECTION_LINE_HEIGHT; row++) {
        StringBuilder nextLine = new StringBuilder();
        for (int i = 0; i < box.lineWidth; i++) {
          nextLine.append(childMidPositionMap.containsKey(i) ? SHU : INDENT);
        }
        box.lines.add(nextLine.toString());
      }
    }

    for (Box child : box.children) {
      buildBoxLines(child);
    }

    for (int i = 0; i < box.getChildrenLineCount(); i++) {
      StringBuilder line = new StringBuilder();
      for (int j = 0; j < box.childCount(); j++) {
        line.append(box.getChild(j).getLine(i));
        if (j != box.childCount() - 1) {
          for (int m = 0; m < BOX_MARGIN; m++) {
            line.append(INDENT);
          }
        }
      }
      box.lines.add(line.toString());
    }
  }

  private static int getChildMidPosition(Box box, int idx) {
    int left = 0;
    for (int i = 0; i < idx; i++) {
      left += box.children.get(i).lineWidth;
      left += BOX_MARGIN;
    }
    left += box.children.get(idx).lineWidth / 2;
    return left;
  }

  private static String printBoxEdge(Box box, boolean isTopEdge) {
    StringBuilder line = new StringBuilder();
    for (int i = 0; i < box.lineWidth; i++) {
      if (i < box.startPosition) {
        line.append(INDENT);
      } else if (i > box.endPosition) {
        line.append(INDENT);
      } else if (i == box.startPosition) {
        line.append(isTopEdge ? LEFT_TOP : LEFT_BOTTOM);
      } else if (i == box.endPosition) {
        line.append(isTopEdge ? RIGHT_TOP : RIGHT_BOTTOM);
      } else {
        line.append(HENG);
      }
    }
    return line.toString();
  }
}
