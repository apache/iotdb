package org.apache.iotdb.db.tools.logvisual;

import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import javax.swing.JFrame;
import javax.swing.JPanel;
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
  private static final String STR_HAPPENED_INSTANCE = "HappenedInstance";

  private LogParser logParser;
  private List<LogEntry> logCache = new ArrayList<>();

  private Map<String, VisualizePlan> plans = new HashMap<>();
  private Map<List<String>, List<LogEntry>> logGroups = new HashMap<>();

  private void clearLogs() {
    logCache.clear();
    logGroups.clear();
  }

  public void loadLogParser(String propertyFilePath, String logFilePath)
      throws IOException {
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

  public void loadPlan(String planFilePath) throws IOException {
    try (FileInputStream reader = new FileInputStream(planFilePath);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(reader)) {
      Properties properties = new Properties();
      properties.load(bufferedInputStream);
      VisualizePlan plan = new VisualizePlan(properties);
      plans.put(plan.getName(), plan);
    }
  }

  public Collection<String> listPlans() {
    return plans.keySet();
  }

  public void executePlan(String planName) throws AnalyzeException {
    VisualizePlan plan = plans.get(planName);
    if (plan == null) {
      throw new NoSuchPlanException(planName);
    }
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
    List<JFreeChart> charts = drawCharts(taggedTimeSeries, plan);
    showCharts(charts.subList(0,10));
  }

  private void collectLogs(VisualizePlan plan) throws AnalyzeException {
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

  private Map<String, TimeSeriesCollection> createTimeSeries(VisualizePlan plan) {
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
          tagTimeseries.addSeries(new TimeSeries(concatenatedTag + legend));
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
          happenedInstance.add(new Millisecond(logEntry.getDate()), 1.0);
        }
      }
      ret.put(concatenatedTag, tagTimeseries);
    }
    return ret;
  }

  private List<JFreeChart> drawCharts(Map<String, TimeSeriesCollection> taggedTimeSeries, VisualizePlan plan) {
    List<JFreeChart> charts = new ArrayList<>();
    for (Entry<String, TimeSeriesCollection> entry : taggedTimeSeries.entrySet()) {
      String tag = entry.getKey();
      TimeSeriesCollection timeSeriesList = entry.getValue();
      if (plan.getMeasurementPositions() != null) {
        // a real-valued timeseries, draw a curve
        JFreeChart chart = ChartFactory.createTimeSeriesChart(tag, "time", "value", timeSeriesList);
        XYPlot xyPlot = chart.getXYPlot();
        ((XYLineAndShapeRenderer) xyPlot.getRenderer()).setDefaultShapesFilled(true);
        charts.add(chart);
      } else {
        // a binary timeseries, draw a scatter plot
        JFreeChart chart = ChartFactory.createScatterPlot(tag, "time", "", timeSeriesList);
        charts.add(chart);
      }
    }
    return charts;
  }

  private void showCharts(List<JFreeChart> charts) {
    for (JFreeChart chart : charts) {
      BufferedImage image = chart.createBufferedImage(800, 600);
      JFrame frame = new JFrame();
      frame.add(new JPanel() {
        @Override
        protected void paintComponent(Graphics g) {
          g.drawImage(image, 0, 0, null);
        }
      });
      frame.setVisible(true);
      frame.setSize(880, 660);
    }
  }

  public static void main(String[] args) throws IOException, AnalyzeException {
    LogVisualizer visualizer = new LogVisualizer();
    visualizer.loadLogParser("E:\\codestore\\incubator-iotdb\\server\\src\\assembly\\resources"
        + "\\tools\\logAnalyze\\default.log.pattern",
        "C:\\Users\\admin\\Desktop\\logs\\log-all-2019-08-21.0.log");
    visualizer.loadPlan("E:\\codestore\\incubator-iotdb\\server\\src\\assembly\\resources\\tools\\logAnalyze\\plans\\flushTimeConsumption.plan");
    visualizer.executePlan("flushTimeConsumption");
  }
}