package org.apache.iotdb.db.storageengine.dataregion.compaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution.ColumnRename;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution.TableRename;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.fileset.TsFileSet;
import org.apache.iotdb.db.utils.EncryptDBUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.exception.write.NoTableException;
import org.apache.tsfile.file.metadata.ColumnSchemaBuilder;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Test;

public class CompactionWithSevoTest extends AbstractCompactionTest{

  @Test
  public void testReadChunkCompactionPerformer() throws Exception {
    testInner(targets ->new ReadChunkCompactionPerformer(seqResources, targets, EncryptDBUtils.getDefaultFirstEncryptParam()), CompactionTaskSummary::new);
  }

  @Test
  public void testReadPointCompactionPerformerSeq() throws Exception {
    testInner(targets ->new ReadPointCompactionPerformer(seqResources, Collections.emptyList(), targets), CompactionTaskSummary::new);
  }

  @Test
  public void testReadPointCompactionPerformerUnseq() throws Exception {
    testInner(targets ->new ReadPointCompactionPerformer(Collections.emptyList(), seqResources, targets), CompactionTaskSummary::new);
  }

  @Test
  public void testReadPointCompactionPerformerCross() throws Exception {
    testCross(targets ->new ReadPointCompactionPerformer(seqResources, unseqResources, targets), CompactionTaskSummary::new);
  }

  @Test
  public void testFastCompactionPerformerSeq() throws Exception {
    testInner(targets -> new FastCompactionPerformer(seqResources, Collections.emptyList(), targets, EncryptDBUtils.getDefaultFirstEncryptParam()), FastCompactionTaskSummary::new);
  }

  @Test
  public void testFastCompactionPerformerUnseq() throws Exception {
    testInner(targets -> new FastCompactionPerformer(Collections.emptyList(), seqResources, targets, EncryptDBUtils.getDefaultFirstEncryptParam()), FastCompactionTaskSummary::new);
  }

  @Test
  public void testFastCompactionPerformerCross() throws Exception {
    testCross(targets -> new FastCompactionPerformer(seqResources, unseqResources, targets, EncryptDBUtils.getDefaultFirstEncryptParam()), FastCompactionTaskSummary::new);
  }

  private void genSourceFiles() throws Exception{
    String fileSetDir =
        TestConstant.BASE_OUTPUT_PATH + File.separator + TsFileSet.FILE_SET_DIR_NAME;
    // seq-file1:
    // table1[s1, s2, s3]
    // table2[s1, s2, s3]
    File seqf1 = new File(SEQ_DIRS, "0-1-0-0.tsfile");
    TableSchema tableSchema1_1 =
        new TableSchema(
            "table1",
            Arrays.asList(
                new ColumnSchemaBuilder()
                    .name("s1")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s2")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s3")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build()));
    TableSchema tableSchema1_2 =
        new TableSchema(
            "table2",
            Arrays.asList(
                new ColumnSchemaBuilder()
                    .name("s1")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s2")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s3")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build()));
    try (TsFileWriter tsFileWriter = new TsFileWriter(seqf1)) {
      tsFileWriter.registerTableSchema(tableSchema1_1);
      tsFileWriter.registerTableSchema(tableSchema1_2);

      Tablet tablet1 = new Tablet(tableSchema1_1.getTableName(), tableSchema1_1.getColumnSchemas());
      tablet1.addTimestamp(0, 0);
      tablet1.addValue(0, 0, 1);
      tablet1.addValue(0, 1, 2);
      tablet1.addValue(0, 2, 3);

      Tablet tablet2 = new Tablet(tableSchema1_2.getTableName(), tableSchema1_2.getColumnSchemas());
      tablet2.addTimestamp(0, 0);
      tablet2.addValue(0, 0, 101);
      tablet2.addValue(0, 1, 102);
      tablet2.addValue(0, 2, 103);

      tsFileWriter.writeTable(tablet1);
      tsFileWriter.writeTable(tablet2);
    }
    TsFileResource resource1 = new TsFileResource(seqf1);
    resource1.setTsFileManager(tsFileManager);
    resource1.updateStartTime(Factory.DEFAULT_FACTORY.create(new String[]{"table1"}), 0);
    resource1.updateEndTime(Factory.DEFAULT_FACTORY.create(new String[]{"table1"}), 0);
    resource1.updateStartTime(Factory.DEFAULT_FACTORY.create(new String[]{"table2"}), 0);
    resource1.updateEndTime(Factory.DEFAULT_FACTORY.create(new String[]{"table2"}), 0);
    resource1.close();

    // rename table1 -> table0
    TsFileSet tsFileSet1 = new TsFileSet(1, fileSetDir, false);
    tsFileSet1.appendSchemaEvolution(
        Collections.singletonList(new TableRename("table1", "table0")));
    tsFileManager.addTsFileSet(tsFileSet1, 0);

    // seq-file2:
    // table0[s1, s2, s3]
    // table2[s1, s2, s3]
    File seqf2 = new File(SEQ_DIRS, "0-2-0-0.tsfile");
    TableSchema tableSchema2_1 =
        new TableSchema(
            "table0",
            Arrays.asList(
                new ColumnSchemaBuilder()
                    .name("s1")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s2")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s3")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build()));
    TableSchema tableSchema2_2 =
        new TableSchema(
            "table2",
            Arrays.asList(
                new ColumnSchemaBuilder()
                    .name("s1")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s2")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s3")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build()));
    try (TsFileWriter tsFileWriter = new TsFileWriter(seqf2)) {
      tsFileWriter.registerTableSchema(tableSchema2_1);
      tsFileWriter.registerTableSchema(tableSchema2_2);

      Tablet tablet1 = new Tablet(tableSchema2_1.getTableName(), tableSchema2_1.getColumnSchemas());
      tablet1.addTimestamp(0, 1);
      tablet1.addValue(0, 0, 11);
      tablet1.addValue(0, 1, 12);
      tablet1.addValue(0, 2, 13);

      Tablet tablet2 = new Tablet(tableSchema2_2.getTableName(), tableSchema2_2.getColumnSchemas());
      tablet2.addTimestamp(0, 1);
      tablet2.addValue(0, 0, 111);
      tablet2.addValue(0, 1, 112);
      tablet2.addValue(0, 2, 113);

      tsFileWriter.writeTable(tablet1);
      tsFileWriter.writeTable(tablet2);
    }
    TsFileResource resource2 = new TsFileResource(seqf2);
    resource2.setTsFileManager(tsFileManager);
    resource2.updateStartTime(Factory.DEFAULT_FACTORY.create(new String[]{"table0"}), 1);
    resource2.updateEndTime(Factory.DEFAULT_FACTORY.create(new String[]{"table0"}), 1);
    resource2.updateStartTime(Factory.DEFAULT_FACTORY.create(new String[]{"table2"}), 1);
    resource2.updateEndTime(Factory.DEFAULT_FACTORY.create(new String[]{"table2"}), 1);
    resource2.close();


    // rename table0.s1 -> table0.s0
    TsFileSet tsFileSet2 = new TsFileSet(2, fileSetDir, false);
    tsFileSet2.appendSchemaEvolution(
        Collections.singletonList(new ColumnRename("table0", "s1", "s0")));
    tsFileManager.addTsFileSet(tsFileSet2, 0);

    // seq-file3:
    // table0[s0, s2, s3]
    // table2[s1, s2, s3]
    File seqf3 = new File(SEQ_DIRS, "0-3-0-0.tsfile");
    TableSchema tableSchema3_1 =
        new TableSchema(
            "table0",
            Arrays.asList(
                new ColumnSchemaBuilder()
                    .name("s0")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s2")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s3")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build()));
    TableSchema tableSchema3_2 =
        new TableSchema(
            "table2",
            Arrays.asList(
                new ColumnSchemaBuilder()
                    .name("s1")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s2")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s3")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build()));
    try (TsFileWriter tsFileWriter = new TsFileWriter(seqf3)) {
      tsFileWriter.registerTableSchema(tableSchema3_1);
      tsFileWriter.registerTableSchema(tableSchema3_2);

      Tablet tablet1 = new Tablet(tableSchema3_1.getTableName(), tableSchema3_1.getColumnSchemas());
      tablet1.addTimestamp(0, 2);
      tablet1.addValue(0, 0, 21);
      tablet1.addValue(0, 1, 22);
      tablet1.addValue(0, 2, 23);

      Tablet tablet2 = new Tablet(tableSchema3_2.getTableName(), tableSchema3_2.getColumnSchemas());
      tablet2.addTimestamp(0, 2);
      tablet2.addValue(0, 0, 121);
      tablet2.addValue(0, 1, 122);
      tablet2.addValue(0, 2, 123);

      tsFileWriter.writeTable(tablet1);
      tsFileWriter.writeTable(tablet2);
    }
    TsFileResource resource3 = new TsFileResource(seqf3);
    resource3.setTsFileManager(tsFileManager);
    resource3.updateStartTime(Factory.DEFAULT_FACTORY.create(new String[]{"table0"}), 2);
    resource3.updateEndTime(Factory.DEFAULT_FACTORY.create(new String[]{"table0"}), 2);
    resource3.updateStartTime(Factory.DEFAULT_FACTORY.create(new String[]{"table2"}), 2);
    resource3.updateEndTime(Factory.DEFAULT_FACTORY.create(new String[]{"table2"}), 2);
    resource3.close();

    // rename table2 -> table1
    TsFileSet tsFileSet3 = new TsFileSet(3, fileSetDir, false);
    tsFileSet3.appendSchemaEvolution(
        Collections.singletonList(new TableRename("table2", "table1")));
    tsFileManager.addTsFileSet(tsFileSet3, 0);

    seqResources.add(resource1);
    seqResources.add(resource2);
    seqResources.add(resource3);

    // unseq-file4:
    // table0[s0, s2, s3]
    // table1[s1, s2, s3]
    File unseqf4 = new File(UNSEQ_DIRS, "0-4-0-0.tsfile");
    TableSchema tableSchema4_1 =
        new TableSchema(
            "table0",
            Arrays.asList(
                new ColumnSchemaBuilder()
                    .name("s0")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s2")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s3")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build()));
    TableSchema tableSchema4_2 =
        new TableSchema(
            "table1",
            Arrays.asList(
                new ColumnSchemaBuilder()
                    .name("s1")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s2")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("s3")
                    .dataType(TSDataType.INT32)
                    .category(ColumnCategory.FIELD)
                    .build()));
    try (TsFileWriter tsFileWriter = new TsFileWriter(unseqf4)) {
      tsFileWriter.registerTableSchema(tableSchema4_1);
      tsFileWriter.registerTableSchema(tableSchema4_2);

      Tablet tablet1 = new Tablet(tableSchema4_1.getTableName(), tableSchema4_1.getColumnSchemas());
      tablet1.addTimestamp(0, 1);
      tablet1.addValue(0, 0, 1011);
      tablet1.addValue(0, 1, 1012);
      tablet1.addValue(0, 2, 1013);

      Tablet tablet2 = new Tablet(tableSchema4_2.getTableName(), tableSchema4_2.getColumnSchemas());
      tablet2.addTimestamp(0, 1);
      tablet2.addValue(0, 0, 1111);
      tablet2.addValue(0, 1, 1112);
      tablet2.addValue(0, 2, 1113);

      tsFileWriter.writeTable(tablet1);
      tsFileWriter.writeTable(tablet2);
    }
    TsFileResource resource4 = new TsFileResource(unseqf4);
    resource4.setTsFileManager(tsFileManager);
    resource4.updateStartTime(Factory.DEFAULT_FACTORY.create(new String[]{"table0"}), 1);
    resource4.updateEndTime(Factory.DEFAULT_FACTORY.create(new String[]{"table0"}), 1);
    resource4.updateStartTime(Factory.DEFAULT_FACTORY.create(new String[]{"table1"}), 1);
    resource4.updateEndTime(Factory.DEFAULT_FACTORY.create(new String[]{"table1"}), 1);
    resource4.close();
    unseqResources.add(resource4);
  }

  private void testCross(Function<List<TsFileResource>, ICompactionPerformer> compactionPerformerFunction, Supplier<CompactionTaskSummary> summarySupplier) throws Exception {
    genSourceFiles();
    List<TsFileResource> targetResources;
    ICompactionPerformer performer;

    targetResources = CompactionFileGeneratorUtils.getCrossCompactionTargetTsFileResources(seqResources);
    targetResources.forEach(s -> s.setTsFileManager(tsFileManager));

    performer = compactionPerformerFunction.apply(targetResources);
    performer.setSummary(summarySupplier.get());
    performer.perform();

    // target(version=1):
    // table1[s1, s2, s3]
    // table2[s1, s2, s3]
    try (ITsFileReader tsFileReader =
        new TsFileReaderBuilder().file(targetResources.get(0).getTsFile()).build()) {
      // table0 should not exist
      try {
        tsFileReader.query("table0", Collections.singletonList("s2"), Long.MIN_VALUE, Long.MAX_VALUE);
        fail("table0 should not exist");
      } catch (NoTableException e) {
        assertEquals("Table table0 not found", e.getMessage());
      }

      // table1.s0 should not exist
      try {
        tsFileReader.query("table1", Collections.singletonList("s0"), Long.MIN_VALUE, Long.MAX_VALUE);
        fail("table1.s0 should not exist");
      } catch (NoMeasurementException e) {
        assertEquals("No measurement for s0", e.getMessage());
      }

      // check data of table1
      ResultSet resultSet = tsFileReader.query("table1", Arrays.asList("s1", "s2", "s3"),
          Long.MIN_VALUE, Long.MAX_VALUE);
      assertTrue(resultSet.next());
      assertEquals(0, resultSet.getLong(1));
      for (int j = 0; j < 3; j++) {
        assertEquals(j + 1, resultSet.getLong(j + 2));
      }

      // check data of table2
      resultSet = tsFileReader.query("table2", Arrays.asList("s1", "s2", "s3"),
          Long.MIN_VALUE, Long.MAX_VALUE);
      assertTrue(resultSet.next());
      assertEquals(0, resultSet.getLong(1));
      for (int j = 0; j < 3; j++) {
        assertEquals(100 + j + 1, resultSet.getLong(j + 2));
      }
    }

    // target(version=2):
    // table0[s1, s2, s3]
    // table2[s1, s2, s3]
    try (ITsFileReader tsFileReader =
        new TsFileReaderBuilder().file(targetResources.get(1).getTsFile()).build()) {
      // table1 should not exist
      try {
        tsFileReader.query("table1", Collections.singletonList("s2"), Long.MIN_VALUE, Long.MAX_VALUE);
        fail("table1 should not exist");
      } catch (NoTableException e) {
        assertEquals("Table table1 not found", e.getMessage());
      }

      // table0.s0 should not exist
      try {
        tsFileReader.query("table0", Collections.singletonList("s0"), Long.MIN_VALUE, Long.MAX_VALUE);
        fail("table0.s0 should not exist");
      } catch (NoMeasurementException e) {
        assertEquals("No measurement for s0", e.getMessage());
      }

      // check data of table0
      ResultSet resultSet = tsFileReader.query("table0", Arrays.asList("s1", "s2", "s3"),
          Long.MIN_VALUE, Long.MAX_VALUE);
      assertTrue(resultSet.next());
      assertEquals(1, resultSet.getLong(1));
      for (int j = 0; j < 3; j++) {
        assertEquals(1010 + j + 1, resultSet.getLong(j + 2));
      }

      // check data of table2
      resultSet = tsFileReader.query("table2", Arrays.asList("s1", "s2", "s3"),
          Long.MIN_VALUE, Long.MAX_VALUE);
      assertTrue(resultSet.next());
      assertEquals(1, resultSet.getLong(1));
      for (int j = 0; j < 3; j++) {
        assertEquals(1110 + j + 1, resultSet.getLong(j + 2));
      }
    }

    // target(version=2):
    // table0[s0, s2, s3]
    // table2[s1, s2, s3]
    try (ITsFileReader tsFileReader =
        new TsFileReaderBuilder().file(targetResources.get(2).getTsFile()).build()) {
      // table1 should not exist
      try {
        tsFileReader.query("table1", Collections.singletonList("s2"), Long.MIN_VALUE, Long.MAX_VALUE);
        fail("table1 should not exist");
      } catch (NoTableException e) {
        assertEquals("Table table1 not found", e.getMessage());
      }

      // table0.s1 should not exist
      try {
        tsFileReader.query("table0", Collections.singletonList("s1"), Long.MIN_VALUE, Long.MAX_VALUE);
        fail("table0.s0 should not exist");
      } catch (NoMeasurementException e) {
        assertEquals("No measurement for s1", e.getMessage());
      }

      // check data of table0
      ResultSet resultSet = tsFileReader.query("table0", Arrays.asList("s0", "s2", "s3"),
          Long.MIN_VALUE, Long.MAX_VALUE);
      assertTrue(resultSet.next());
      assertEquals(2, resultSet.getLong(1));
      for (int j = 0; j < 3; j++) {
        assertEquals(20 + j + 1, resultSet.getLong(j + 2));
      }

      // check data of table2
      resultSet = tsFileReader.query("table2", Arrays.asList("s1", "s2", "s3"),
          Long.MIN_VALUE, Long.MAX_VALUE);
      assertTrue(resultSet.next());
      assertEquals(2, resultSet.getLong(1));
      for (int j = 0; j < 3; j++) {
        assertEquals(100 + 20 + j + 1, resultSet.getLong(j + 2));
      }
    }
  }

  private void testInner(Function<List<TsFileResource>, ICompactionPerformer> compactionPerformerFunction, Supplier<CompactionTaskSummary> summarySupplier) throws Exception {
    genSourceFiles();
    List<TsFileResource> targetResources;
    ICompactionPerformer performer;

    // target(version=1):
    // table1[s1, s2, s3]
    // table2[s1, s2, s3]
    targetResources = CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources, true);
    targetResources.forEach(s -> s.setTsFileManager(tsFileManager));

    performer = compactionPerformerFunction.apply(targetResources);
    performer.setSummary(summarySupplier.get());
    performer.perform();

    try (ITsFileReader tsFileReader =
        new TsFileReaderBuilder().file(targetResources.get(0).getTsFile()).build()) {
      // table0 should not exist
      try {
        tsFileReader.query("table0", Collections.singletonList("s2"), Long.MIN_VALUE, Long.MAX_VALUE);
        fail("table0 should not exist");
      } catch (NoTableException e) {
        assertEquals("Table table0 not found", e.getMessage());
      }

      // table1.s0 should not exist
      try {
        tsFileReader.query("table1", Collections.singletonList("s0"), Long.MIN_VALUE, Long.MAX_VALUE);
        fail("table1.s0 should not exist");
      } catch (NoMeasurementException e) {
        assertEquals("No measurement for s0", e.getMessage());
      }

      // check data of table1
      ResultSet resultSet = tsFileReader.query("table1", Arrays.asList("s1", "s2", "s3"),
          Long.MIN_VALUE, Long.MAX_VALUE);
      for (int i = 0; i < 3; i++) {
        assertTrue(resultSet.next());
        assertEquals(i, resultSet.getLong(1));
        for (int j = 0; j < 3; j++) {
          assertEquals(i * 10 + j + 1, resultSet.getLong(j + 2));
        }
      }

      // check data of table2
      resultSet = tsFileReader.query("table2", Arrays.asList("s1", "s2", "s3"),
          Long.MIN_VALUE, Long.MAX_VALUE);
      for (int i = 0; i < 3; i++) {
        assertTrue(resultSet.next());
        assertEquals(i, resultSet.getLong(1));
        for (int j = 0; j < 3; j++) {
          assertEquals(100 + i * 10 + j + 1, resultSet.getLong(j + 2));
        }
      }
    }

    // target(version=2):
    // table0[s1, s2, s3]
    // table2[s1, s2, s3]
    targetResources = CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources.subList(1,
        seqResources.size()), true);
    targetResources.forEach(s -> s.setTsFileManager(tsFileManager));

    performer = compactionPerformerFunction.apply(targetResources);
    performer.setSummary(summarySupplier.get());
    performer.perform();

    try (ITsFileReader tsFileReader =
        new TsFileReaderBuilder().file(targetResources.get(0).getTsFile()).build()) {
      // table1 should not exist
      try {
        tsFileReader.query("table1", Collections.singletonList("s2"), Long.MIN_VALUE, Long.MAX_VALUE);
        fail("table1 should not exist");
      } catch (NoTableException e) {
        assertEquals("Table table1 not found", e.getMessage());
      }

      // table0.s0 should not exist
      try {
        tsFileReader.query("table0", Collections.singletonList("s0"), Long.MIN_VALUE, Long.MAX_VALUE);
        fail("table0.s0 should not exist");
      } catch (NoMeasurementException e) {
        assertEquals("No measurement for s0", e.getMessage());
      }

      // check data of table0
      ResultSet resultSet = tsFileReader.query("table0", Arrays.asList("s1", "s2", "s3"),
          Long.MIN_VALUE, Long.MAX_VALUE);
      for (int i = 0; i < 3; i++) {
        assertTrue(resultSet.next());
        assertEquals(i, resultSet.getLong(1));
        for (int j = 0; j < 3; j++) {
          assertEquals(i * 10 + j + 1, resultSet.getLong(j + 2));
        }
      }

      // check data of table2
      resultSet = tsFileReader.query("table2", Arrays.asList("s1", "s2", "s3"),
          Long.MIN_VALUE, Long.MAX_VALUE);
      for (int i = 0; i < 3; i++) {
        assertTrue(resultSet.next());
        assertEquals(i, resultSet.getLong(1));
        for (int j = 0; j < 3; j++) {
          assertEquals(100 + i * 10 + j + 1, resultSet.getLong(j + 2));
        }
      }
    }

    // target(version=2):
    // table0[s0, s2, s3]
    // table2[s1, s2, s3]
    targetResources = CompactionFileGeneratorUtils.getInnerCompactionTargetTsFileResources(seqResources.subList(2,
        seqResources.size()), true);
    targetResources.forEach(s -> s.setTsFileManager(tsFileManager));

    performer = compactionPerformerFunction.apply(targetResources);
    performer.setSummary(summarySupplier.get());
    performer.perform();

    try (ITsFileReader tsFileReader =
        new TsFileReaderBuilder().file(targetResources.get(0).getTsFile()).build()) {
      // table1 should not exist
      try {
        tsFileReader.query("table1", Collections.singletonList("s2"), Long.MIN_VALUE, Long.MAX_VALUE);
        fail("table1 should not exist");
      } catch (NoTableException e) {
        assertEquals("Table table1 not found", e.getMessage());
      }

      // table0.s1 should not exist
      try {
        tsFileReader.query("table0", Collections.singletonList("s1"), Long.MIN_VALUE, Long.MAX_VALUE);
        fail("table0.s0 should not exist");
      } catch (NoMeasurementException e) {
        assertEquals("No measurement for s1", e.getMessage());
      }

      // check data of table0
      ResultSet resultSet = tsFileReader.query("table0", Arrays.asList("s0", "s2", "s3"),
          Long.MIN_VALUE, Long.MAX_VALUE);
      for (int i = 0; i < 3; i++) {
        assertTrue(resultSet.next());
        assertEquals(i, resultSet.getLong(1));
        for (int j = 0; j < 3; j++) {
          assertEquals(i * 10 + j + 1, resultSet.getLong(j + 2));
        }
      }

      // check data of table2
      resultSet = tsFileReader.query("table2", Arrays.asList("s1", "s2", "s3"),
          Long.MIN_VALUE, Long.MAX_VALUE);
      for (int i = 0; i < 3; i++) {
        assertTrue(resultSet.next());
        assertEquals(i, resultSet.getLong(1));
        for (int j = 0; j < 3; j++) {
          assertEquals(100 + i * 10 + j + 1, resultSet.getLong(j + 2));
        }
      }
    }
  }
}
