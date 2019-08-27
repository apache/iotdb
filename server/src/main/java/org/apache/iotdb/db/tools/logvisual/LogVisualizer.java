package org.apache.iotdb.db.tools.logvisual;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.iotdb.db.tools.logvisual.exceptions.AnalyzeException;
import org.apache.iotdb.db.tools.logvisual.exceptions.NoLogFileLoadedException;
import org.apache.iotdb.db.tools.logvisual.exceptions.NoSuchPlanException;
import org.apache.iotdb.db.tools.logvisual.exceptions.UnmatchedContentException;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogVisualizer {

  private static final Logger logger = LoggerFactory.getLogger(LogVisualizer.class);

  private LogParser logParser;
  private List<LogEntry> logCache = new ArrayList<>();

  private Map<String, VisualizationPlan> plans = new HashMap<>();
  private Map<List<String>, List<LogEntry>> logGroups = new HashMap<>();

  private Map<String, JFreeChart> charts;
  private Map<String, List<TimeSeriesStatistics>> statisticsMap;

  private File parserPropertyFile;
  private File logFile;

  private void clearLogs() {
    logCache.clear();
    logGroups.clear();
  }

  public void loadLogParser() throws IOException {
    if (parserPropertyFile == null) {
      throw new IOException("Parser property file unset!");
    }
    if (logFile == null) {
      throw new IOException("Log file unset!");
    }
    String propertyFilePath = parserPropertyFile.getPath();
    String logFilePath = logFile.getPath();
    Properties properties = new Properties();
    try (FileInputStream inputStream = new FileInputStream(propertyFilePath);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream)) {
      properties.load(bufferedInputStream);
    }
    logParser = new PatternLogParser(properties, logFilePath);
  }

  public void close() throws IOException {
    logParser.close();
    logParser = null;
  }

  public void loadPlans(File[] planFiles) throws IOException {
    for (File file : planFiles) {
      loadPlan(file);
    }
  }

  public void loadPlan(File file) throws IOException {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      loadPlans(file.listFiles());
    } else {
      loadPlan(file.getPath());
    }
  }

  private void loadPlan(String planFilePath) throws IOException {
    VisualizationPlan plan = new VisualizationPlan(planFilePath);
    plans.put(plan.getName(), plan);
  }

  public Collection<VisualizationPlan> listPlans() {
    return plans.values();
  }

  public void executePlan(String planName) throws AnalyzeException {
    VisualizationPlan plan = plans.get(planName);
    if (plan == null) {
      throw new NoSuchPlanException(planName);
    }
    executePlan(plan);
  }

  public void executePlan(VisualizationPlan plan) throws AnalyzeException {
    if (logParser == null) {
      throw new NoLogFileLoadedException();
    }
    try {
      logParser.reset();
    } catch (IOException e) {
      throw new AnalyzeException(e);
    }
    collectLogs(plan);
    groupLogs();
    Map<String,TimeSeriesCollection> taggedTimeSeries = createTimeSeries(plan);
    charts = drawCharts(taggedTimeSeries, plan);
    statisticsMap = genStatisticMap(taggedTimeSeries);
  }

  private void collectLogs(VisualizationPlan plan) throws AnalyzeException {
    LogFilter logFilter = plan.getLogFilter();
    clearLogs();
    try {
      LogEntry logEntry;
      readLogs:
      while ((logEntry = logParser.next()) != null) {
        try {
          plan.parseContents(logEntry);
        } catch (UnmatchedContentException e) {
          continue;
        }

        switch (logFilter.filter(logEntry)) {
          case BEYOND_END_TIME:
            break readLogs;
          case REJECT:
            break;
          case OK:
            logCache.add(logEntry);
        }
      }
    } catch (IOException e) {
      throw new AnalyzeException(e);
    }
    logger.info("Collected {} logs", logCache.size());
  }

  private void groupLogs() {
    for (LogEntry logEntry : logCache) {
      logGroups.computeIfAbsent(logEntry.getTags(), tag -> new ArrayList<>()).add(logEntry);
    }
    logger.info("Found {} different tags", logGroups.size());
  }

  private Map<String, TimeSeriesCollection> createTimeSeries(VisualizationPlan plan) {
    Map<String, TimeSeriesCollection> ret = new HashMap<>();
    for (Entry<List<String>, List<LogEntry>> entry : logGroups.entrySet()) {
      List<String> tags = entry.getKey();
      List<LogEntry> logs = entry.getValue();
      String concatenatedTag;
      if (tags.isEmpty()) {
        concatenatedTag = plan.getName();
      } else {
        StringBuilder builder = new StringBuilder(plan.getName() + "-" + tags.get(0));
        for (int i = 1; i < tags.size(); i++) {
          builder.append(",").append(tags.get(i));
        }
        builder.append(" ");
        concatenatedTag = builder.toString();
      }
      TimeSeriesCollection tagTimeseries = new TimeSeriesCollection();
      if (plan.getMeasurementPositions() != null) {
        String[] legends = plan.getLegends();
        for (String legend : legends) {
          TimeSeries timeSeries = new TimeSeries(concatenatedTag + legend);
          tagTimeseries.addSeries(timeSeries);
        }
        for (LogEntry logEntry : logs) {
          List<Double> values = logEntry.getMeasurements();
          for (int i = 0; i < values.size(); i++) {
            tagTimeseries.getSeries(i).addOrUpdate(new Millisecond(logEntry.getDate()), values.get(i));
          }
        }
      } else {
        TimeSeries happenedInstance = new TimeSeries("HappenedInstance");
        for (LogEntry logEntry : logs) {
          happenedInstance.addOrUpdate(new Millisecond(logEntry.getDate()), 1.0);
        }
        tagTimeseries.addSeries(happenedInstance);
      }
      ret.put(concatenatedTag, tagTimeseries);
    }
    return ret;
  }

  private Map<String, JFreeChart> drawCharts(Map<String, TimeSeriesCollection> taggedTimeSeries,
      VisualizationPlan plan) {
    Map<String, JFreeChart> charts = new HashMap<>();
    for (Entry<String, TimeSeriesCollection> entry : taggedTimeSeries.entrySet()) {
      String tag = entry.getKey();
      TimeSeriesCollection timeSeriesList = entry.getValue();
      Date startDate = new Date((long) timeSeriesList.getDomainBounds(true).getLowerBound());
      if (plan.getMeasurementPositions() != null) {
        // a real-valued timeseries, draw a curve
        JFreeChart chart = ChartFactory.createTimeSeriesChart(tag, "time-"+ startDate, "value",
            timeSeriesList);
        XYPlot xyPlot = chart.getXYPlot();
        XYLineAndShapeRenderer xyLineAndShapeRenderer = ((XYLineAndShapeRenderer) xyPlot
            .getRenderer());
        xyLineAndShapeRenderer.setDefaultShapesVisible(true);
        xyLineAndShapeRenderer.setDefaultShapesFilled(true);

        charts.put(tag, chart);
      } else {
        JFreeChart chart = ChartFactory.createTimeSeriesChart(tag, "time-"+ startDate, "value",
            timeSeriesList);
        XYPlot xyPlot = chart.getXYPlot();
        XYLineAndShapeRenderer xyLineAndShapeRenderer = ((XYLineAndShapeRenderer) xyPlot
            .getRenderer());
        xyLineAndShapeRenderer.setDefaultShapesVisible(true);
        xyLineAndShapeRenderer.setDefaultShapesFilled(true);
        xyLineAndShapeRenderer.setDefaultLinesVisible(false);

        charts.put(tag, chart);
      }
    }
    return charts;
  }

  public Map<String, JFreeChart> getCharts() {
    return charts;
  }

  public void setParserPropertyFile(File parserPropertyFile) {
    this.parserPropertyFile = parserPropertyFile;
  }

  public void setLogFile(File logFile) {
    this.logFile = logFile;
  }

  private Map<String, List<TimeSeriesStatistics>> genStatisticMap(Map<String,TimeSeriesCollection>
      taggedTimeSeries) {
    Map<String, List<TimeSeriesStatistics>> ret = new HashMap<>();
    for (Entry<String, TimeSeriesCollection> timeSeriesCollectionEntry : taggedTimeSeries.entrySet()) {
      String tag = timeSeriesCollectionEntry.getKey();
      TimeSeriesCollection timeSeriesCollection = timeSeriesCollectionEntry.getValue();
      List<TimeSeriesStatistics> seriesStatistics = new ArrayList<>();
      for (int i = 0; i < timeSeriesCollection.getSeriesCount(); i++) {
        TimeSeries timeSeries = timeSeriesCollection.getSeries(i);
        seriesStatistics.add(new TimeSeriesStatistics(timeSeries));
      }
      ret.put(tag, seriesStatistics);
    }
    return ret;
  }

  public Map<String, List<TimeSeriesStatistics>> getStatisticsMap() {
    return statisticsMap;
  }

  public static void main(String[] args) throws IOException, AnalyzeException {
    LogVisualizer visualizer = new LogVisualizer();
    visualizer.setParserPropertyFile(new File("E:\\codestore\\incubator-iotdb\\server\\src\\assembly"
        + "\\resources"
        + "\\tools\\logAnalyze\\default.log.pattern"));
    visualizer.setLogFile( new File("C:\\Users\\admin\\Desktop\\logs\\log-all-2019-08-21.0.log"));
    visualizer.loadLogParser();
    visualizer.loadPlan("E:\\codestore\\incubator-iotdb\\server\\src\\assembly\\resources\\tools\\logAnalyze\\plans\\flushTimeConsumption.plan");
    visualizer.executePlan("flushTimeConsumption");
  }
}