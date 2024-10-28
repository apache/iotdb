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

package org.apache.iotdb.db.metadata.schemaRegion;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.ITimeSeriesSchemaInfo;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("java:S5783")
public class SchemaRegionAliasAndTagTest extends AbstractSchemaRegionTest {

  private static final Logger logger = LoggerFactory.getLogger(SchemaRegionAliasAndTagTest.class);
  private ISchemaRegion schemaRegion;

  public SchemaRegionAliasAndTagTest(SchemaRegionTestParams testParams) {
    super(testParams);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    schemaRegion = getSchemaRegion("root.sg", 0);
  }

  /**
   * Prepare timeseries
   *
   * <p>"root.sg.wf01.wt01.v1.s1" without tags, attributes and alias
   *
   * <p>"root.sg.wf01.wt01.v1.s2" with tags, attributes and alias
   *
   * <p>"root.sg.wf01.aligned_device1.(s1,s2)" aligned without tags, attributes and alias
   *
   * <p>"root.sg.wf01.aligned_device2.(s1,s2)" aligned with tags, attributes and alias
   */
  private void prepareTimeseries() throws MetadataException {
    SchemaRegionTestUtil.createTimeseries(
        schemaRegion,
        "root.sg.wf01.wt01.v1.s1",
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        CompressionType.SNAPPY,
        null,
        null,
        null,
        null);
    SchemaRegionTestUtil.createTimeseries(
        schemaRegion,
        "root.sg.wf01.wt01.v1.s2",
        TSDataType.FLOAT,
        TSEncoding.RLE,
        CompressionType.GZIP,
        null,
        new HashMap<String, String>() {
          {
            put("tag1", "t1");
            put("tag2", "t2");
          }
        },
        new HashMap<String, String>() {
          {
            put("attr1", "a1");
            put("attr2", "a2");
          }
        },
        "temp");
    SchemaRegionTestUtil.createAlignedTimeseries(
        schemaRegion,
        "root.sg.wf01.aligned_device1",
        Arrays.asList("s1", "s2"),
        Arrays.asList(TSDataType.INT64, TSDataType.INT32),
        Arrays.asList(TSEncoding.PLAIN, TSEncoding.RLE),
        Arrays.asList(CompressionType.SNAPPY, CompressionType.LZ4),
        null,
        null,
        null);
    SchemaRegionTestUtil.createAlignedTimeseries(
        schemaRegion,
        "root.sg.wf01.aligned_device2",
        Arrays.asList("s1", "s2"),
        Arrays.asList(TSDataType.INT64, TSDataType.INT32),
        Arrays.asList(TSEncoding.PLAIN, TSEncoding.RLE),
        Arrays.asList(CompressionType.SNAPPY, CompressionType.LZ4),
        Arrays.asList(
            new HashMap<String, String>() {
              {
                put("tag1", "t1");
              }
            },
            new HashMap<String, String>() {
              {
                put("tag2", "t2");
              }
            }),
        Arrays.asList(
            new HashMap<String, String>() {
              {
                put("attr1", "a1");
              }
            },
            new HashMap<String, String>() {
              {
                put("attr2", "a2");
              }
            }),
        Arrays.asList("alias1", "alias2"));
  }

  private void checkAliasAndTagsAndAttributes(
      String fullPath, String alias, Map<String, String> tags, Map<String, String> attributes) {
    try {
      List<ITimeSeriesSchemaInfo> result =
          SchemaRegionTestUtil.showTimeseries(schemaRegion, new PartialPath(fullPath));
      Assert.assertEquals(1, result.size());
      Assert.assertEquals(fullPath, result.get(0).getFullPath());
      Assert.assertEquals(alias, result.get(0).getAlias());
      Assert.assertEquals(tags, result.get(0).getTags());
      Assert.assertEquals(attributes, result.get(0).getAttributes());
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      Assert.fail(e.getMessage());
    }
  }

  private void checkAttributes(String fullPath, Map<String, String> attributes) {
    try {
      List<ITimeSeriesSchemaInfo> result =
          SchemaRegionTestUtil.showTimeseries(schemaRegion, new PartialPath(fullPath));
      Assert.assertEquals(1, result.size());
      Assert.assertEquals(fullPath, result.get(0).getFullPath());
      Assert.assertEquals(attributes, result.get(0).getAttributes());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  private void checkTags(String fullPath, Map<String, String> tags) {
    try {
      List<ITimeSeriesSchemaInfo> result =
          SchemaRegionTestUtil.showTimeseries(schemaRegion, new PartialPath(fullPath));
      Assert.assertEquals(1, result.size());
      Assert.assertEquals(fullPath, result.get(0).getFullPath());
      Assert.assertEquals(tags, result.get(0).getTags());
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testShowTimeseries() throws MetadataException {
    prepareTimeseries();
    checkAliasAndTagsAndAttributes(
        "root.sg.wf01.wt01.v1.s1", null, Collections.emptyMap(), Collections.emptyMap());
    checkAliasAndTagsAndAttributes(
        "root.sg.wf01.wt01.v1.s2",
        "temp",
        new HashMap<String, String>() {
          {
            put("tag1", "t1");
            put("tag2", "t2");
          }
        },
        new HashMap<String, String>() {
          {
            put("attr1", "a1");
            put("attr2", "a2");
          }
        });
    checkAliasAndTagsAndAttributes(
        "root.sg.wf01.aligned_device1.s1", null, Collections.emptyMap(), Collections.emptyMap());
    checkAliasAndTagsAndAttributes(
        "root.sg.wf01.aligned_device1.s2", null, Collections.emptyMap(), Collections.emptyMap());
    checkAliasAndTagsAndAttributes(
        "root.sg.wf01.aligned_device2.s1",
        "alias1",
        new HashMap<String, String>() {
          {
            put("tag1", "t1");
          }
        },
        new HashMap<String, String>() {
          {
            put("attr1", "a1");
          }
        });
    checkAliasAndTagsAndAttributes(
        "root.sg.wf01.aligned_device2.s2",
        "alias2",
        new HashMap<String, String>() {
          {
            put("tag2", "t2");
          }
        },
        new HashMap<String, String>() {
          {
            put("attr2", "a2");
          }
        });
  }

  @Test
  public void testUpsertAliasAndTagsAndAttributes() {
    try {
      prepareTimeseries();
      try {
        schemaRegion.upsertAliasAndTagsAndAttributes(
            "s2", null, null, new PartialPath("root.sg.wf01.wt01.v1.s1"));
        Assert.fail();
      } catch (final Exception e) {
        Assert.assertTrue(
            e.getMessage()
                .contains("The alias is duplicated with the name or alias of other measurement"));
      }
      try {
        schemaRegion.upsertAliasAndTagsAndAttributes(
            "temp", null, null, new PartialPath("root.sg.wf01.wt01.v1.s1"));
        Assert.fail();
      } catch (final Exception e) {
        Assert.assertTrue(
            e.getMessage()
                .contains("The alias is duplicated with the name or alias of other measurement"));
      }

      final Map<String, String> newTags =
          new HashMap<String, String>() {
            {
              put("tag1", "new1");
              put("tag2", "new2");
              put("tag3", "new3");
            }
          };
      final Map<String, String> newAttributes =
          new HashMap<String, String>() {
            {
              put("attr1", "new1");
              put("attr2", "new2");
              put("attr3", "new3");
            }
          };
      final List<String> fullPaths =
          Arrays.asList(
              "root.sg.wf01.wt01.v1.s1",
              "root.sg.wf01.wt01.v1.s2",
              "root.sg.wf01.aligned_device1.s1",
              "root.sg.wf01.aligned_device1.s2",
              "root.sg.wf01.aligned_device2.s1",
              "root.sg.wf01.aligned_device2.s2");
      final List<String> aliasList =
          Arrays.asList(
              "newAlias1", "newAlias2", "newAlias3", "newAlias4", "newAlias5", "newAlias6");
      for (int i = 0; i < fullPaths.size(); i++) {
        schemaRegion.upsertAliasAndTagsAndAttributes(
            aliasList.get(i), newTags, newAttributes, new PartialPath(fullPaths.get(i)));
        checkAliasAndTagsAndAttributes(fullPaths.get(i), aliasList.get(i), newTags, newAttributes);
      }
    } catch (final Exception e) {
      logger.error(e.getMessage(), e);
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testAddAttributes() {
    try {
      prepareTimeseries();
      Map<String, String> newAttributes =
          new HashMap<String, String>() {
            {
              put("attr2", "new2");
              put("attr3", "new3");
            }
          };
      schemaRegion.addAttributes(newAttributes, new PartialPath("root.sg.wf01.wt01.v1.s1"));
      schemaRegion.addAttributes(newAttributes, new PartialPath("root.sg.wf01.aligned_device1.s1"));
      try {
        schemaRegion.addAttributes(newAttributes, new PartialPath("root.sg.wf01.wt01.v1.s2"));
        Assert.fail();
      } catch (MetadataException e) {
        // expected
        Assert.assertTrue(e.getMessage().contains("already has the attribute"));
      }
      try {
        schemaRegion.addAttributes(
            newAttributes, new PartialPath("root.sg.wf01.aligned_device2.s2"));
        Assert.fail();
      } catch (MetadataException e) {
        // expected
        Assert.assertTrue(e.getMessage().contains("already has the attribute"));
      }
      checkAttributes("root.sg.wf01.wt01.v1.s1", newAttributes);
      checkAttributes("root.sg.wf01.aligned_device1.s1", newAttributes);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testAddTags() {
    try {
      prepareTimeseries();
      Map<String, String> newTags =
          new HashMap<String, String>() {
            {
              put("tag2", "new2");
              put("tag3", "new3");
            }
          };
      schemaRegion.addTags(newTags, new PartialPath("root.sg.wf01.wt01.v1.s1"));
      schemaRegion.addTags(newTags, new PartialPath("root.sg.wf01.aligned_device1.s1"));
      try {
        schemaRegion.addTags(newTags, new PartialPath("root.sg.wf01.wt01.v1.s2"));
        Assert.fail();
      } catch (MetadataException e) {
        // expected
        Assert.assertTrue(e.getMessage().contains("already has the tag"));
      }
      try {
        schemaRegion.addTags(newTags, new PartialPath("root.sg.wf01.aligned_device2.s2"));
        Assert.fail();
      } catch (MetadataException e) {
        // expected
        Assert.assertTrue(e.getMessage().contains("already has the tag"));
      }
      checkTags("root.sg.wf01.wt01.v1.s1", newTags);
      checkTags("root.sg.wf01.aligned_device1.s1", newTags);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testDropTagsOrAttributes() {
    try {
      // create timeseries with tags and attributes
      prepareTimeseries();
      // add tags and attributes after create
      Set<String> keySet =
          new HashSet<>(Arrays.asList("tag1", "tag2", "tag3", "attr1", "attr2", "attr3"));
      Map<String, String> newTags =
          new HashMap<String, String>() {
            {
              put("tag2", "new2");
              put("tag3", "new3");
            }
          };
      schemaRegion.addTags(newTags, new PartialPath("root.sg.wf01.wt01.v1.s1"));
      schemaRegion.addTags(newTags, new PartialPath("root.sg.wf01.aligned_device1.s1"));
      Map<String, String> newAttributes =
          new HashMap<String, String>() {
            {
              put("attr2", "new2");
              put("attr3", "new3");
            }
          };
      schemaRegion.addAttributes(newAttributes, new PartialPath("root.sg.wf01.wt01.v1.s1"));
      schemaRegion.addAttributes(newAttributes, new PartialPath("root.sg.wf01.aligned_device1.s1"));
      // drop all tags and attributes then check
      List<String> fullPaths =
          Arrays.asList(
              "root.sg.wf01.wt01.v1.s1",
              "root.sg.wf01.wt01.v1.s2",
              "root.sg.wf01.aligned_device1.s1",
              "root.sg.wf01.aligned_device1.s2",
              "root.sg.wf01.aligned_device2.s1",
              "root.sg.wf01.aligned_device2.s2");
      for (String fullPath : fullPaths) {
        schemaRegion.dropTagsOrAttributes(keySet, new PartialPath(fullPath));
        checkTags(fullPath, Collections.emptyMap());
        checkAttributes(fullPath, Collections.emptyMap());
      }
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testSetTagsOrAttributesValue() {
    try {
      prepareTimeseries();
      Map<String, String> tagsOrAttributesValue =
          new HashMap<String, String>() {
            {
              put("tag1", "new1");
              put("attr1", "new1");
            }
          };
      List<String> fullPaths =
          Arrays.asList(
              "root.sg.wf01.wt01.v1.s1",
              "root.sg.wf01.wt01.v1.s2",
              "root.sg.wf01.aligned_device1.s1",
              "root.sg.wf01.aligned_device1.s2",
              "root.sg.wf01.aligned_device2.s1",
              "root.sg.wf01.aligned_device2.s2");
      List<Boolean> expectException = Arrays.asList(true, false, true, true, false, true);
      for (int i = 0; i < fullPaths.size(); i++) {
        try {
          schemaRegion.setTagsOrAttributesValue(
              tagsOrAttributesValue, new PartialPath(fullPaths.get(i)));
          Assert.assertFalse(expectException.get(i));
        } catch (Exception e) {
          Assert.assertTrue(expectException.get(i));
          Assert.assertTrue(e.getMessage().contains("does not have"));
        }
      }
      checkAttributes(
          "root.sg.wf01.wt01.v1.s2",
          new HashMap<String, String>() {
            {
              put("attr1", "new1");
              put("attr2", "a2");
            }
          });
      checkTags(
          "root.sg.wf01.wt01.v1.s2",
          new HashMap<String, String>() {
            {
              put("tag1", "new1");
              put("tag2", "t2");
            }
          });
      checkAttributes(
          "root.sg.wf01.aligned_device2.s1",
          new HashMap<String, String>() {
            {
              put("attr1", "new1");
            }
          });
      checkTags(
          "root.sg.wf01.aligned_device2.s1",
          new HashMap<String, String>() {
            {
              put("tag1", "new1");
            }
          });
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testRenameTagOrAttributeKey() {
    try {
      prepareTimeseries();
      try {
        schemaRegion.renameTagOrAttributeKey(
            "attr1", "attr2", new PartialPath("root.sg.wf01.wt01.v1.s2"));
        Assert.fail();
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage().contains("already has a tag/attribute named"));
      }
      try {
        schemaRegion.renameTagOrAttributeKey(
            "attr3", "newAttr3", new PartialPath("root.sg.wf01.wt01.v1.s2"));
        Assert.fail();
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage().contains("does not have tag/attribute"));
      }
      schemaRegion.renameTagOrAttributeKey(
          "attr1", "newAttr1", new PartialPath("root.sg.wf01.wt01.v1.s2"));
      schemaRegion.renameTagOrAttributeKey(
          "tag1", "newTag1", new PartialPath("root.sg.wf01.wt01.v1.s2"));
      schemaRegion.renameTagOrAttributeKey(
          "attr1", "newAttr1", new PartialPath("root.sg.wf01.aligned_device2.s1"));
      schemaRegion.renameTagOrAttributeKey(
          "tag1", "newTag1", new PartialPath("root.sg.wf01.aligned_device2.s1"));
      checkAttributes(
          "root.sg.wf01.wt01.v1.s2",
          new HashMap<String, String>() {
            {
              put("newAttr1", "a1");
              put("attr2", "a2");
            }
          });
      checkTags(
          "root.sg.wf01.wt01.v1.s2",
          new HashMap<String, String>() {
            {
              put("newTag1", "t1");
              put("tag2", "t2");
            }
          });
      checkAttributes(
          "root.sg.wf01.aligned_device2.s1",
          new HashMap<String, String>() {
            {
              put("newAttr1", "a1");
            }
          });
      checkTags(
          "root.sg.wf01.aligned_device2.s1",
          new HashMap<String, String>() {
            {
              put("newTag1", "t1");
            }
          });
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testDeleteAndQueryByAlias() {
    try {
      prepareTimeseries();
      List<ITimeSeriesSchemaInfo> result =
          SchemaRegionTestUtil.showTimeseries(
              schemaRegion, new PartialPath("root.sg.wf01.wt01.v1.temp"));
      Assert.assertEquals(1, result.size());
      // Delete timeseries
      final PathPatternTree patternTree = new PathPatternTree();
      patternTree.appendFullPath(new PartialPath("root.sg.wf01.wt01.v1.temp"));
      patternTree.constructTree();
      Assert.assertTrue(schemaRegion.constructSchemaBlackList(patternTree).getLeft() >= 1);
      schemaRegion.deleteTimeseriesInBlackList(patternTree);
      result =
          SchemaRegionTestUtil.showTimeseries(
              schemaRegion, new PartialPath("root.sg.wf01.wt01.v1.temp"));
      Assert.assertEquals(0, result.size());
    } catch (final Exception e) {
      logger.error(e.getMessage(), e);
      Assert.fail(e.getMessage());
    }
  }
}
