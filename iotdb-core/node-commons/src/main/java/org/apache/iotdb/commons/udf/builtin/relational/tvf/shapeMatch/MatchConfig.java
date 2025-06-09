package org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch;

public class MatchConfig {
  public static final int gapTolerance =
      2; // if the gap between new two points is larger than this times gap before, the new point
  // will consider as a new segment start

  public static final int lineSectionTolerance =
      5; // if the point num more than this while it's heightBound is in smoothValue, it will be
  // considered as a line section

  public static final int shapeTolerance = 0;

  public static final Boolean calcSEusingMoreMemory = true;
}
