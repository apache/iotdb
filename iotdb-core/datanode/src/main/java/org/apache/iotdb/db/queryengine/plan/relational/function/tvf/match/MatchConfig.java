package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.match;

public class MatchConfig {
  // if the gap between new two points is larger than this times gap before, the new point
  // will consider as a new segment start
  public static final int GAP_TOLERANCE = 2;

  // if the point num more than this while it's heightBound is in smoothValue, it will be
  // considered as a line section
  public static final int LINE_SECTION_TOLERANCE = 4;

  public static final int SHAPE_TOLERANCE = 0;

  public static final boolean CALC_SE_USING_MORE_MEMORY = true;
}
