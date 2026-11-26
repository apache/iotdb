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

package org.apache.iotdb.commons.schema.filter.impl;

import org.apache.iotdb.commons.path.ExtendedPartialPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.fa.IFAState;
import org.apache.iotdb.commons.path.fa.IFATransition;
import org.apache.iotdb.commons.path.fa.IPatternFA;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.NotFilter;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.TagFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.InFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.PreciseFilter;
import org.apache.iotdb.commons.schema.tree.AbstractTreeVisitor;
import org.apache.iotdb.commons.schema.tree.ITreeNode;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DeviceFilterUtilTest {

  private static final String[] PREFIX = new String[] {"root", "db", "table"};
  private static final int TAG_COLUMN_NUM = 3;

  @Test
  public void testCompactExpandedInOrOnNonLeadingTag() {
    final List<PartialPath> patterns =
        convert(branch(precise(1, "card1")), branch(precise(1, "card2")));

    Assert.assertEquals(1, patterns.size());
    final ExtendedPartialPath pattern = (ExtendedPartialPath) patterns.get(0);
    Assert.assertEquals("root.db.table.*.*.*", pattern.getFullPath());
    Assert.assertTrue(pattern.match(PREFIX.length + 1, "card1"));
    Assert.assertTrue(pattern.match(PREFIX.length + 1, "card2"));
    Assert.assertFalse(pattern.match(PREFIX.length + 1, "card3"));
    assertPreciseTransitionsAfterFirstTag(pattern, "card1", "card2");
  }

  @Test
  public void testKeepLeadingTagBranchesSeparated() {
    final List<PartialPath> patterns =
        convert(branch(precise(0, "meter1")), branch(precise(0, "meter2")));

    Assert.assertEquals(2, patterns.size());
    Assert.assertEquals("root.db.table.meter1.*.*", patterns.get(0).getFullPath());
    Assert.assertEquals("root.db.table.meter2.*.*", patterns.get(1).getFullPath());
  }

  @Test
  public void testKeepBranchesSeparatedWithCompletePrefix() {
    final List<PartialPath> patterns =
        convert(
            branch(precise(0, "meter1"), precise(1, "card1")),
            branch(precise(0, "meter1"), precise(1, "card2")));

    Assert.assertEquals(2, patterns.size());
    Assert.assertEquals("root.db.table.meter1.card1.*", patterns.get(0).getFullPath());
    Assert.assertEquals("root.db.table.meter1.card2.*", patterns.get(1).getFullPath());
  }

  @Test
  public void testKeepFullyPreciseBranchesSeparatedWithManyTags() {
    final int tagColumnNum = 32;
    final List<SchemaFilter> firstBranch = new ArrayList<>(tagColumnNum);
    final List<SchemaFilter> secondBranch = new ArrayList<>(tagColumnNum);
    for (int i = 0; i < tagColumnNum; i++) {
      firstBranch.add(precise(i, "value" + i));
      secondBranch.add(precise(i, i == tagColumnNum - 1 ? "other" : "value" + i));
    }

    final List<PartialPath> patterns = convert(tagColumnNum, firstBranch, secondBranch);

    Assert.assertEquals(2, patterns.size());
    Assert.assertEquals("value31", patterns.get(0).getNodes()[PREFIX.length + 31]);
    Assert.assertEquals("other", patterns.get(1).getNodes()[PREFIX.length + 31]);
  }

  @Test
  public void testCompactPreciseBranchesAfterLongNonPrecisePrefix() {
    final int tagColumnNum = 32;
    final List<PartialPath> patterns =
        convert(
            tagColumnNum,
            branch(precise(16, "common"), precise(31, "last1")),
            branch(precise(16, "common"), precise(31, "last2")));

    Assert.assertEquals(1, patterns.size());
    final ExtendedPartialPath pattern = (ExtendedPartialPath) patterns.get(0);
    Assert.assertTrue(pattern.match(PREFIX.length + 16, "common"));
    Assert.assertTrue(pattern.match(PREFIX.length + 31, "last1"));
    Assert.assertTrue(pattern.match(PREFIX.length + 31, "last2"));
    Assert.assertFalse(pattern.match(PREFIX.length + 31, "last3"));
  }

  @Test
  public void testCompactCartesianProductBehindWildcard() {
    final List<PartialPath> patterns =
        convert(
            branch(precise(1, "card1"), precise(2, "device1")),
            branch(precise(1, "card1"), precise(2, "device2")),
            branch(precise(1, "card2"), precise(2, "device1")),
            branch(precise(1, "card2"), precise(2, "device2")));

    Assert.assertEquals(1, patterns.size());
    final ExtendedPartialPath pattern = (ExtendedPartialPath) patterns.get(0);
    Assert.assertTrue(pattern.match(PREFIX.length + 1, "card1"));
    Assert.assertTrue(pattern.match(PREFIX.length + 1, "card2"));
    Assert.assertTrue(pattern.match(PREFIX.length + 2, "device1"));
    Assert.assertTrue(pattern.match(PREFIX.length + 2, "device2"));
  }

  @Test
  public void testDoNotCompactCorrelatedOrBranches() {
    final List<PartialPath> patterns =
        convert(
            branch(precise(1, "card1"), precise(2, "device1")),
            branch(precise(1, "card2"), precise(2, "device2")));

    Assert.assertEquals(2, patterns.size());
  }

  @Test
  public void testDoNotCompactNullPreciseValue() {
    final List<PartialPath> patterns =
        convert(branch(precise(1, null)), branch(precise(1, "card1")));

    Assert.assertEquals(2, patterns.size());
  }

  @Test
  public void testCombineMultiExactAndOtherFilters() {
    final List<PartialPath> patterns =
        convert(
            branch(
                new TagFilter(new InFilter(new HashSet<>(Arrays.asList("card1", "card2"))), 1),
                new TagFilter(new NotFilter(new PreciseFilter("card2")), 1)));

    Assert.assertEquals(1, patterns.size());
    assertPreciseTransitionsAfterFirstTag(patterns.get(0), "card1");
  }

  @Test
  public void testIterateMultiExactValuesWhenTheyAreFewer() {
    final TestNode root = new TestNode("root");
    final TestNode parent = new TestNode("meter");
    root.addChild(parent);
    parent.addChildren("card1", "card2", "card3", "card4");

    final TestVisitor visitor =
        new TestVisitor(root, createAdaptivePattern(Set.of("card1", "card2")), parent);

    Assert.assertEquals(Arrays.asList("card1", "card2"), collect(visitor));
    Assert.assertEquals(2, visitor.getTargetDirectLookupCount());
    Assert.assertEquals(0, visitor.getTargetChildrenIterationCount());
  }

  @Test
  public void testIterateChildKeysWhenTheyAreFewer() {
    final TestNode root = new TestNode("root");
    final TestNode parent = new TestNode("meter");
    root.addChild(parent);
    parent.addChildren("card1", "card2");

    final TestVisitor visitor =
        new TestVisitor(
            root,
            createAdaptivePattern(Set.of("card1", "card2", "card3", "card4", "card5")),
            parent);

    Assert.assertEquals(Arrays.asList("card1", "card2"), collect(visitor));
    Assert.assertEquals(0, visitor.getTargetDirectLookupCount());
    Assert.assertEquals(1, visitor.getTargetChildrenIterationCount());
  }

  @Test
  public void testIterateChildKeysWhenCandidateCountsAreEqual() {
    final TestNode root = new TestNode("root");
    final TestNode parent = new TestNode("meter");
    root.addChild(parent);
    parent.addChildren("card1", "card2");

    final TestVisitor visitor =
        new TestVisitor(root, createAdaptivePattern(Set.of("card1", "card2")), parent);

    Assert.assertEquals(Arrays.asList("card1", "card2"), collect(visitor));
    Assert.assertEquals(0, visitor.getTargetDirectLookupCount());
    Assert.assertEquals(1, visitor.getTargetChildrenIterationCount());
  }

  @Test
  public void testUseDirectLookupForSingleCandidate() {
    final TestNode root = new TestNode("root");
    final TestNode parent = new TestNode("meter");
    root.addChild(parent);
    parent.addChildren("card1");

    final TestVisitor visitor =
        new TestVisitor(root, createAdaptivePattern(Set.of("card1")), parent);

    Assert.assertEquals(Collections.singletonList("card1"), collect(visitor));
    Assert.assertEquals(1, visitor.getTargetDirectLookupCount());
    Assert.assertEquals(0, visitor.getTargetChildrenIterationCount());
  }

  private static ExtendedPartialPath createAdaptivePattern(final Set<String> values) {
    final ExtendedPartialPath pattern =
        new ExtendedPartialPath(new String[] {"root", "*", "*"}, true);
    pattern.addMultiExactMatch(2, values);
    return pattern;
  }

  private static List<String> collect(final TestVisitor visitor) {
    final List<String> result = new ArrayList<>();
    try {
      while (visitor.hasNext()) {
        result.add(visitor.next());
      }
    } finally {
      visitor.close();
    }
    Collections.sort(result);
    return result;
  }

  @SafeVarargs
  private static List<PartialPath> convert(final List<SchemaFilter>... branches) {
    return convert(TAG_COLUMN_NUM, branches);
  }

  @SafeVarargs
  private static List<PartialPath> convert(
      final int tagColumnNum, final List<SchemaFilter>... branches) {
    return DeviceFilterUtil.convertToDevicePattern(
        PREFIX, tagColumnNum, Arrays.asList(branches), false);
  }

  private static List<SchemaFilter> branch(final SchemaFilter... filters) {
    return Arrays.asList(filters);
  }

  private static TagFilter precise(final int index, final String value) {
    return new TagFilter(new PreciseFilter(value), index);
  }

  private static void assertPreciseTransitionsAfterFirstTag(
      final PartialPath pattern, final String... expectedTransitions) {
    final IPatternFA patternFA = new IPatternFA.Builder().pattern(pattern).buildNFA();
    IFAState state = patternFA.getInitialState();
    for (final String prefixNode : PREFIX) {
      final IFATransition transition = patternFA.getPreciseMatchTransition(state).get(prefixNode);
      Assert.assertNotNull(transition);
      state = patternFA.getNextState(state, transition);
    }
    final IFATransition wildcardTransition =
        patternFA.getFuzzyMatchTransitionIterator(state).next();
    state = patternFA.getNextState(state, wildcardTransition);

    final Set<String> expected = new HashSet<>(Arrays.asList(expectedTransitions));
    Assert.assertEquals(expected, patternFA.getPreciseMatchTransition(state).keySet());
    Assert.assertEquals(0, patternFA.getFuzzyMatchTransitionSize(state));
  }

  private static class TestVisitor extends AbstractTreeVisitor<TestNode, String> {

    private final TestNode targetParent;
    private int targetDirectLookupCount;
    private int targetChildrenIterationCount;

    private TestVisitor(
        final TestNode root, final ExtendedPartialPath pathPattern, final TestNode targetParent) {
      super(root, pathPattern, false);
      this.targetParent = targetParent;
      initStack();
    }

    @Override
    protected TestNode getChild(final TestNode parent, final String childName) {
      if (parent == targetParent) {
        targetDirectLookupCount++;
      }
      return parent.children.get(childName);
    }

    @Override
    protected Iterator<TestNode> getChildrenIterator(final TestNode parent) {
      if (parent == targetParent) {
        targetChildrenIterationCount++;
      }
      return parent.children.values().iterator();
    }

    @Override
    protected Iterator<TestNode> getChildrenIterator(
        final TestNode parent, final Iterator<String> childrenName) {
      final List<TestNode> children = new ArrayList<>();
      childrenName.forEachRemaining(
          name -> {
            final TestNode child = parent.children.get(name);
            if (child != null) {
              children.add(child);
            }
          });
      return children.iterator();
    }

    @Override
    protected int getChildrenSize(final TestNode parent) {
      return parent.children.keySet().size();
    }

    @Override
    protected boolean shouldVisitSubtreeOfInternalMatchedNode(final TestNode node) {
      return true;
    }

    @Override
    protected boolean shouldVisitSubtreeOfFullMatchedNode(final TestNode node) {
      return false;
    }

    @Override
    protected boolean acceptInternalMatchedNode(final TestNode node) {
      return false;
    }

    @Override
    protected boolean acceptFullMatchedNode(final TestNode node) {
      return true;
    }

    @Override
    protected String generateResult(final TestNode nextMatchedNode) {
      return nextMatchedNode.getName();
    }

    @Override
    protected boolean mayTargetNodeType(final TestNode node) {
      return true;
    }

    private int getTargetDirectLookupCount() {
      return targetDirectLookupCount;
    }

    private int getTargetChildrenIterationCount() {
      return targetChildrenIterationCount;
    }
  }

  private static class TestNode implements ITreeNode {

    private final String name;
    private final Map<String, TestNode> children = new LinkedHashMap<>();

    private TestNode(final String name) {
      this.name = name;
    }

    private void addChild(final TestNode child) {
      children.put(child.getName(), child);
    }

    private void addChildren(final String... childNames) {
      Arrays.stream(childNames).map(TestNode::new).forEach(this::addChild);
    }

    @Override
    public String getName() {
      return name;
    }
  }
}
