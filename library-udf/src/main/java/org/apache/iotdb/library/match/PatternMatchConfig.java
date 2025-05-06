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

package org.apache.iotdb.library.match;

public class PatternMatchConfig {

  // Minimum height (in percentage, related to the entire section) that is needed to create a new
  // section
  // This is to avoid that very small sections that are similar to a horizontal line create many
  // sections
  // Without it the algorithm that divides the sequence in sections would be too much sensible to
  // noises
  // was 0.01 for tests
  public static final double DIVIDE_SECTION_MIN_HEIGHT_DATA = 0.01; // 这个是消除序列数据的噪声

  // 0.01 for tests, 0.1 for 1NN
  public static final double DIVIDE_SECTION_MIN_HEIGHT_QUERY = 0.01; // 这个是消除模板数据的噪声

  public static final int MAX_REGEX_IT = 25; // 正则表达式匹配的最大迭代次数 TODO 真的有正则表达式？

  // query compatibility
  /**
   * if the number of sections is N, and the number of sections with a different sign is D. The
   * algorithm consider the two subsequences as incompatible if D/N > 0.5
   */
  public static final double QUERY_SIGN_MAXIMUM_TOLERABLE_DIFFERENT_SIGN_SECTIONS = 0.5; // 这个是兼容性匹配的阈值，超过这个阈值的说明不需要比较，肯定是不匹配的

  /**
   * keep only one (best) match if the same area is selected in different smooth iterations with not
   * experiments it's better a false, so every smooth iteration has a not match, so they are easier
   * to view
   */
  public static final boolean REMOVE_EQUAL_MATCHES = false; // 是否去除有重合的匹配 TODO 去重保留的策略是什么

  /** true for tests, false for 1NN */
  public static final boolean CHECK_QUERY_COMPATIBILITY = true; // 这个是兼容性匹配检查的开关

  /** the first and last sections are cut to have a good fit */
  public static final boolean START_END_CUT_IN_SUBPARTS = true; // 考虑序列段数据的首尾比较粗糙，这里设置是否进行切除，在匹配的时候能过有更好的效果。 TODO 这里需要看是否是对模板数据进行切除，看这个使用怪怪的

  /**
   * the first and last sections are cut as well in the results, or are returned highlighting the
   * whole section. false in tests
   */
  public static final boolean START_END_CUT_IN_SUBPARTS_IN_RESULTS = true; // 这里考虑是否对已经匹配的结果的首尾进行切除。(就是切除首尾匹配成功后，是要记录原始点还是记录切除后的点)

  /** true for tests */
  public static final boolean RESCALING_Y = true; // 这里是指在局部缩放的时候，是否对Y轴进行缩放，如果选择false，那么使用的是X轴的缩放比例

  public static final int VALUE_DIFFERENCE_WEIGHT = 1; // 这个是值的差异权重，越大说明对值的差异越敏感
  public static final int RESCALING_COST_WEIGHT = 1; // 这个是重缩放的权重，越大说明对重缩放的敏感度越高
}
