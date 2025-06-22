package org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.model;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.MatchConfig.lineSectionTolerance;

public class PatternSegment {
  // variable area
  private List<Point> points = new ArrayList<>();
  private Double totalHeight = 0.0;
  private List<Section> sections = new ArrayList<>();

  private Boolean isConstantChar = false;
  private Character constantChar = null;

  // constant area
  public PatternSegment() {}

  public PatternSegment(Character c) {
    this.isConstantChar = true;
    this.constantChar = c;
  }

  // variable get/set area
  public Boolean isConstantChar() {
    return isConstantChar;
  }

  public Character getConstantChar() {
    return constantChar;
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
              && concatResult.get(0).getPoints().size() <= lineSectionTolerance) {
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
              if (concatResult.get(0).getPoints().size() <= lineSectionTolerance) {
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
            && sections.get(sections.size() - 2).getPoints().size() <= lineSectionTolerance) {
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

  public void trans2SectionList(String Type, double smoothValue) {
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
          if (Type.equals("series")) {
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

    // connect the sections in the same dataSegment
    Section prev = null;
    for (Section s : sections) {
      if (prev != null) {
        prev.getNextSectionList().add(s);
      }
      prev = s;
    }

    return;
  }

  // address the concat of two dataSegment without combine the two dataSegment
  private void concat(PatternSegment patternSegment) {
    // dataSegment is the prev one of this, so the result is dataSegment + this
    // check whether the last section of dataSegment and the first section of this are the same sign
    Section lastSection = patternSegment.getSections().get(patternSegment.getSections().size() - 1);
    Section firstSection = this.sections.get(0);

    Point lastPoint = lastSection.getPoints().get(lastSection.getPoints().size() - 1);
    Point firstPoint = firstSection.getPoints().get(0);

    // connect the two point at the same x-axis and calc the upHeight
    double upHeight = Math.abs(firstPoint.y - lastPoint.y);

    if (lastSection.getSign() != firstSection.getSign()) {
      lastSection.getNextSectionList().add(0, firstSection);
    } else {
      // need to combine the last section of dataSegment and the first section of this
      // if the last section has out point and the first section has in point
      Section section = lastSection.copy();

      for (int i = 1; i < section.getPoints().size(); i++) {
        section.addPoint(
            new Point(section.getPoints().get(i).x, section.getPoints().get(i).y + upHeight));
      }

      // connect the last 2 section to sectionNow to first 2 section
      patternSegment
          .getSections()
          .get(patternSegment.getSections().size() - 2)
          .getNextSectionList()
          .add(0, section);
      section.getNextSectionList().add(0, this.sections.get(1));

      // set the upHeight to the next section
      this.sections.get(1).setUpHeight(upHeight);
    }
  }

  public void concatNear(PatternSegment patternSegment) {
    concat(patternSegment);
    // combine the two sections of dataSegment and this
    patternSegment.getSections().addAll(this.getSections());
    this.sections = patternSegment.getSections();
  }

  public void concatHeadAndTail() {
    concat(this);
  }
}
