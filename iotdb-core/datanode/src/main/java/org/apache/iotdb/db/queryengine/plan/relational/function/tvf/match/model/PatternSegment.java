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

package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.match.model;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.relational.function.tvf.match.MatchConfig.LINE_SECTION_TOLERANCE;

public class PatternSegment {
  // variable area
  private final List<Point> points = new ArrayList<>();
  private double totalHeight = 0.0;
  private List<Section> sections = new ArrayList<>();
  private List<Section> startSectionList = new ArrayList<>();
  private List<Section> endSectionList = new ArrayList<>();

  private boolean isConstantChar = false;
  private String repeatSign = null;
  private boolean isZeroRepeat = false; // deal with + repeat

  // constant area
  public PatternSegment() {}

  public PatternSegment(String repeatSign) {
    this.isConstantChar = true;
    this.repeatSign = repeatSign;
  }

  // variable get/set area
  public boolean isConstantChar() {
    return isConstantChar;
  }

  public void setIsZeroRepeat(boolean isZeroRepeat) {
    this.isZeroRepeat = isZeroRepeat;
  }

  public String getRepeatSign() {
    return repeatSign;
  }

  public List<Point> getPoints() {
    return points;
  }

  public void addPoint(Point point) {
    this.points.add(point);
    if (point.y > totalHeight) {
      totalHeight = point.y;
    }
  }

  public List<Section> getSections() {
    return this.sections;
  }

  public List<Section> getStartSectionList() {
    return startSectionList;
  }

  public List<Section> getEndSectionList() {
    return endSectionList;
  }

  // special function area
  public static double tangent(Point p1, Point p2) {
    return (p2.y - p1.y) / (p2.x - p1.x);
  }

  private void closeLastSection(double smoothValue) {
    if (sections.get(sections.size() - 1).getHeightBound() <= smoothValue) {
      sections.get(sections.size() - 1).setSign(0);
    }

    if (sections.size() >= 2 && sections.get(sections.size() - 2).getSign() == 0) {
      if (sections.get(sections.size() - 1).getSign() == 0) {
        List<Section> concatResult =
            sections
                .get(sections.size() - 2)
                .concat(sections.get(sections.size() - 1), smoothValue);

        sections.remove(sections.size() - 1);
        sections.remove(sections.size() - 1);
        if (concatResult.size() == 3) {
          if (!sections.isEmpty()
              && concatResult.get(0).getPoints().size() <= LINE_SECTION_TOLERANCE) {
            sections.get(sections.size() - 1).combine(concatResult.get(0));
            if (sections.get(sections.size() - 1).getSign() == concatResult.get(1).getSign()) {
              sections.get(sections.size() - 1).combine(concatResult.get(1));
            } else {
              sections.add(concatResult.get(1));
            }
            sections.add(concatResult.get(2));
          } else {
            sections.add(concatResult.get(0));
            sections.add(concatResult.get(1));
            sections.add(concatResult.get(2));
          }
        } else if (concatResult.size() == 2) {
          if (sections.isEmpty()) {
            sections.add(concatResult.get(0));
            sections.add(concatResult.get(1));
          } else {
            if (concatResult.get(0).getSign() == 0) {
              if (concatResult.get(0).getPoints().size() <= LINE_SECTION_TOLERANCE) {
                sections.get(sections.size() - 1).combine(concatResult.get(0));
                if (sections.get(sections.size() - 1).getSign() == concatResult.get(1).getSign()) {
                  sections.get(sections.size() - 1).combine(concatResult.get(1));
                } else {
                  sections.add(concatResult.get(1));
                }
              } else {
                sections.add(concatResult.get(0));
                sections.add(concatResult.get(1));
              }
            } else {
              if (sections.get(sections.size() - 1).getSign() == concatResult.get(0).getSign()) {
                sections.get(sections.size() - 1).combine(concatResult.get(0));
              } else {
                sections.add(concatResult.get(0));
              }
              sections.add(concatResult.get(1));
            }
          }
        } else {
          sections.add(concatResult.get(0));
        }
      } else {
        if (sections.size() >= 3
            && sections.get(sections.size() - 2).getPoints().size() <= LINE_SECTION_TOLERANCE) {
          sections.get(sections.size() - 3).combine(sections.get(sections.size() - 2));
          sections.remove(sections.size() - 2);
          if (sections.get(sections.size() - 2).getSign()
              == sections.get(sections.size() - 1).getSign()) {
            sections.get(sections.size() - 2).combine(sections.get(sections.size() - 1));
            sections.remove(sections.size() - 1);
          }
        }
      }
    }
  }

  public void trans2SectionList(boolean isPatternFromOrigin, double smoothValue) {
    // check whether the points.size() > 2
    if (points.size() < 2) {
      return;
    }

    Point lastPt = null;
    double lastSign = 0;

    // the tangent is calc by the point before the current point, so the first point is not
    // included, and need to add it into the section
    for (int i = 1; i < points.size(); i++) {
      double tangent = tangent(points.get(i - 1), points.get(i));
      Point pt = points.get(i);
      double sign = Math.signum(tangent);

      // only the same sign will be in the same section
      if (sections.isEmpty()) {
        sections.add(new Section(sign));
        sections.get(0).addPoint(points.get(0));
      } else {
        if (sign != lastSign) {
          if (isPatternFromOrigin) {
            closeLastSection(smoothValue);
          }
          sections.add(new Section(sign));
          sections.get(sections.size() - 1).addPoint(lastPt);
        }
      }

      sections.get(sections.size() - 1).addPoint(pt);
      lastPt = pt;
      lastSign = sign;
    }
    if (isPatternFromOrigin) {
      closeLastSection(smoothValue);
    }

    // connect the sections in the same dataSegment
    Section prev = null;
    for (Section s : sections) {
      if (prev != null) {
        prev.getNextSectionList().add(s);
        s.getPrevSectionList().add(prev);
      }
      prev = s;
    }

    startSectionList.add(sections.get(0));
    endSectionList.add(sections.get(sections.size() - 1));
  }

  // address the concat of two dataSegment without combine the two dataSegment
  private void concat(PatternSegment patternSegment, boolean isUpdateZeroRepeat) {
    List<Section> newStartSection = new ArrayList<>(patternSegment.getStartSectionList());
    List<Section> newEndSection = new ArrayList<>(endSectionList);

    for (Section lastSection : patternSegment.getEndSectionList()) {
      for (Section firstSection : startSectionList) {
        if (lastSection.getSign() != firstSection.getSign()) {
          lastSection.getNextSectionList().add(0, firstSection);
          firstSection.getPrevSectionList().add(lastSection);
        } else {
          Point lastPoint = lastSection.getPoints().get(lastSection.getPoints().size() - 1);
          Point firstPoint = firstSection.getPoints().get(0);

          // connect the two point at the same x-axis and calc the upHeight
          double upHeight = lastPoint.y - firstPoint.y;

          Section section = lastSection.copy();

          for (int k = 1; k < firstSection.getPoints().size(); k++) {
            section.addPoint(
                new Point(lastPoint.x + k, firstSection.getPoints().get(k).y + upHeight));
          }

          if (lastSection.getPrevSectionList() != null
              && !lastSection.getPrevSectionList().isEmpty()) {
            for (Section prevSection : lastSection.getPrevSectionList()) {
              prevSection.getNextSectionList().add(0, section);
              section.getPrevSectionList().add(prevSection);
            }
          } else {
            newStartSection.add(section);
          }

          if (firstSection.getNextSectionList() != null
              && !firstSection.getNextSectionList().isEmpty()) {
            for (Section nextSection : firstSection.getNextSectionList()) {
              section.getNextSectionList().add(nextSection);
              nextSection.getPrevSectionList().add(section);
            }
          } else {
            newEndSection.add(section);
          }
          sections.add(section);
        }
      }
    }

    if (isUpdateZeroRepeat) {
      if (patternSegment.isZeroRepeat) {
        newStartSection.addAll(startSectionList);
      }
      if (isZeroRepeat) {
        newEndSection.addAll(patternSegment.getEndSectionList());
      }
      isZeroRepeat = patternSegment.isZeroRepeat && isZeroRepeat;
    }

    startSectionList = newStartSection;
    endSectionList = newEndSection;
  }

  public void concatNear(PatternSegment patternSegment) {
    concat(patternSegment, true);
    // combine the two sections of dataSegment and this
    patternSegment.getSections().addAll(this.getSections());
    this.sections = patternSegment.getSections();
  }

  public void concatHeadAndTail() {
    concat(this, false);
  }
}
