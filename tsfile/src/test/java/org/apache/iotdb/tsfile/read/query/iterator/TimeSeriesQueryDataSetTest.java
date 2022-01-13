package org.apache.iotdb.tsfile.read.query.iterator;

import org.apache.calcite.DataContext;
import org.apache.calcite.DataContexts;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.IMetadataQuerier;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.reader.LocalTsFileInput;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.LocalNioTsFileOutput;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TimeSeriesQueryDataSetTest {

  static class TestTimeSeries implements TimeSeries {

    private final List<Object[]> values;
    private int index;

    public TestTimeSeries() {
      values = Arrays.asList(
          new Object[]{1L, 1L, (float) 1.5},
          new Object[]{2L, 2L, (float) 3.0}
      );
      index = 0;
    }

    @Override
    public TSDataType[] getSpecification() {
      return new TSDataType[]{
          TSDataType.INT32
//          TSDataType.FLOAT
      };
    }

    @Override
    public boolean hasNext() {
      return index < values.size();
    }

    @Override
    public Object[] next() {
      index++;
      return (Object[]) this.values.get(index - 1);
    }
  }

  public static class TimeSeriesFactory implements AutoCloseable {

    private static FileSystem fs = FileSystems.getDefault();
    private final LocalTsFileInput fileInput;

    public TimeSeriesFactory() {
      try {
        generate(10);

        fileInput = new LocalTsFileInput(fs.getPath("/tmp/test.tsfile"));


      } catch (IOException e) {
        throw new IllegalStateException();
      }
    }

    public TimeSeries create(String device, String measurement) {
      try {
      TsFileSequenceReader fileSequenceReader = new TsFileSequenceReader(fileInput);
      IMetadataQuerier metadataQuerier = new MetadataQuerierByFileImpl(fileSequenceReader);
      List<IChunkMetadata> chunkMetadataList = null;
      CachedChunkLoaderImpl chunkLoader = new CachedChunkLoaderImpl(fileSequenceReader);

        chunkMetadataList = metadataQuerier.getChunkMetaDataList(new Path(device, measurement));

        SeriesIterator iterator = new SeriesIterator(chunkLoader, chunkMetadataList, null);

        return iterator;
      } catch (IOException e) {
        throw new IllegalStateException();
      }
    }

    private void generate(int records) throws IOException {
      // Write a file first
      org.apache.iotdb.tsfile.write.schema.Schema schema = new org.apache.iotdb.tsfile.write.schema.Schema();
      TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
      schema.registerTimeseries(
          new Path("d1"),
          new MeasurementSchema(
              "s1", TSDataType.INT64, TSEncoding.valueOf(conf.getValueEncoder())));
      schema.registerTimeseries(
          new Path("d1"),
          new MeasurementSchema(
              "s2", TSDataType.FLOAT, TSEncoding.valueOf(conf.getValueEncoder())));

      java.nio.file.Path path = fs.getPath("/tmp");
      if (!Files.exists(path)) {
        Files.createDirectory(path);
      }
      path = fs.getPath("/tmp/test.tsfile");
      if (Files.exists(path)) {
        Files.delete(path);
      }

      Random random = new Random();
      try (TsFileWriter writer = new TsFileWriter(new LocalNioTsFileOutput(path), schema)) {
        for (long ts = 1; ts <= records; ts++) {
          TSRecord record = new TSRecord(ts, "d1");
//        if (ts % 2 == 1) {
          record.addTuple(new LongDataPoint("s1", (long)(random.nextDouble() * 100)));
//        }
//        }
//          if (ts % 2 == 0) {
            record.addTuple(new FloatDataPoint("s2", (float)random.nextFloat()));
//          }
          writer.write(record);
        }
      } catch (WriteProcessException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void close() throws Exception {
    }
  }

  @Test
  public void testDataset() throws IOException {
    TimeSeriesQueryDataSet dataSet = new TimeSeriesQueryDataSet(new TestTimeSeries());

    while (dataSet.hasNext()) {
      RowRecord next = dataSet.next();
      System.out.println("Record: " + next);
    }
  }

  public class SeriesScan implements ScannableTable {

    private final String path;
    private final TSDataType[] dataTypes;

    public SeriesScan(String path, TSDataType... dataTypes) {
      this.path = path;
      this.dataTypes = dataTypes;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.createStructType(
          Arrays.asList(
              typeFactory.createJavaType(Long.class),
              typeFactory.createJavaType(Long.class)
//              typeFactory.createJavaType(Float.class)
          ), Arrays.asList(
              "time",
              "value"
//              "float"
          ));
    }

    @Override
    public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }

    @Override
    public Schema.TableType getJdbcTableType() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRolledUp(String column) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, @Nullable SqlNode parent, @Nullable CalciteConnectionConfig config) {
      throw new UnsupportedOperationException();
    }

    public TSDataType[] getDataTypes() {
      return this.dataTypes;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
      // Fetch the TimeSeries, usually one would have used path here
      String[] split = this.path.split("\\.");
      return seriesToEnumerable(factory.create(split[0], split[1]));
    }
  }

  public static Enumerable<Object[]> seriesToEnumerable(TimeSeries series) {
    return Linq4j.asEnumerable(new Iterable<Object[]>() {
      @Override
      public Iterator<Object[]> iterator() {
        return series;
      }
    });
  }

  public class CalciteBasedOptimizer {

    private final JavaTypeFactoryImpl typeFactory;
    private final RexBuilder rexBuilder;
    private final CalciteSchema rootSchema;
    private final CalciteConnectionConfigImpl calciteConnectionConfig;
    private final CalciteCatalogReader catalogReader;
    private final DataContext dataContext;

    public CalciteBasedOptimizer() {
      typeFactory = new JavaTypeFactoryImpl();
      rexBuilder = new RexBuilder(typeFactory);
      rootSchema = CalciteSchema.createRootSchema(false);
      // Register all here
      rootSchema.add("d1.s1", new SeriesScan("d1.s1", TSDataType.INT32));
      rootSchema.add("d1.s2", new SeriesScan("d1.s2", TSDataType.INT32));
      // ...
      calciteConnectionConfig = new CalciteConnectionConfigImpl(new Properties());
      catalogReader = new CalciteCatalogReader(rootSchema, Collections.emptyList(), typeFactory, calciteConnectionConfig);
      // Here we have our context
      DataContext inner = DataContexts.of(rootSchema.getTableNames().stream().collect(Collectors.toMap(Function.identity(), name -> rootSchema.getTable(name, false).getTable())));

      dataContext = new DataContext() {

        @Override
        public @Nullable SchemaPlus getRootSchema() {
          return inner.getRootSchema();
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
          return typeFactory;
        }

        @Override
        public QueryProvider getQueryProvider() {
          return inner.getQueryProvider();
        }

        @Override
        public @Nullable Object get(String name) {
          return inner.get(name);
        }
      };
    }

    public DataContext getContext() {
      return dataContext;
    }

    private EnumeratorDataSet optimize(String seriesName) {
      FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
          .defaultSchema(rootSchema.plus())
          .typeSystem(RelDataTypeSystem.DEFAULT)
          .build();

      RelBuilder relBuilder = RelBuilder.create(frameworkConfig);
      RelOptCluster cluster = relBuilder.getCluster();
      RelOptPlanner planner = cluster.getPlanner();

      RelOptUtil.registerDefaultRules(planner, false, true);
      planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

      RelTraitSet desired = cluster.traitSet().replace(BindableConvention.INSTANCE);

      Table table = rootSchema.getTable(seriesName, false).getTable();

      RexBuilder rexBuilder = relBuilder.getRexBuilder();
      RelNode root = relBuilder
          .scan("d1.s1")
          .scan("d1.s2")
          .join(JoinRelType.LEFT, "time")
          .project(
              relBuilder.field("time"),
              relBuilder.cast(
                  relBuilder.call(
                      SqlStdOperatorTable.MULTIPLY,
                      relBuilder.field("value"),
                      rexBuilder.makeLiteral(2.0, typeFactory.createJavaType(Float.class))),
                  SqlTypeName.INTEGER
              ),
              relBuilder.field(2)
          )
          .build();

      RelNode expectedRoot = planner.changeTraits(root, desired);
      planner.setRoot(expectedRoot);

      RelNode exp = planner.findBestExp();
      Bindable bestExp = (Bindable) exp;

      System.out.println(RelOptUtil.toString(root));
      System.out.println(RelOptUtil.toString(exp));

      Hook.JAVA_PLAN.addThread((Consumer<? extends Object>) System.out::println);

      Enumerable<@Nullable Object[]> enumerable = bestExp.bind(this.getContext());
      Enumerator<@Nullable Object[]> enumerator = enumerable.enumerator();

      SeriesScan scan = (SeriesScan) table;

      return new EnumeratorDataSet(new TSDataType[]{TSDataType.INT64, TSDataType.INT32, TSDataType.INT64}, enumerator);
    }
  }

  private TimeSeriesFactory factory;

  @Before
  public void setUp() throws Exception {
    factory = new TimeSeriesFactory();
  }

  @After
  public void tearDown() throws Exception {
    factory.close();
  }

  @Test
  public void doSomethingWithCalcite() throws IOException {
    Hook.JAVA_PLAN.addThread((Consumer<? extends Object>) System.out::println);

    CalciteBasedOptimizer optimizer = new CalciteBasedOptimizer();

    EnumeratorDataSet dataSet = optimizer.optimize("d1.s1");

    while (dataSet.hasNext()) {
      RowRecord next = dataSet.next();

      System.out.println("Next: " + next);
    }
  }
}