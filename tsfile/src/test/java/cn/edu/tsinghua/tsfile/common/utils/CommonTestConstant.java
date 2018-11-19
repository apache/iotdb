package cn.edu.tsinghua.tsfile.common.utils;

import java.util.Random;

/**
 * This class is used for Junit test to get some unified constant.
 * 
 * @author kangrong
 *
 */
public class CommonTestConstant {
  public static final float float_min_delta = 0.00001f;
  public static final double double_min_delta = 0.00001d;
  public static final Random random = new Random(System.currentTimeMillis());
}
