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
package org.apache.iotdb.web.grafana.dao.impl;

import java.sql.DatabaseMetaData;
import java.time.Duration;
import org.apache.iotdb.jdbc.Constant;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.web.grafana.bean.TimeValues;
import org.apache.iotdb.web.grafana.dao.BasicDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Repository
@PropertySource("classpath:application.properties")
public class BasicDaoImpl implements BasicDao {

  private static final Logger logger = LoggerFactory.getLogger(BasicDaoImpl.class);

  public static final String FALLBACK_AGG_FUNCTION = "LAST";

  private final JdbcTemplate jdbcTemplate;

  private static long TIMESTAMP_RADIX = 1L;

  @Value("${isDownSampling}")
  private boolean isDownSampling;

  @Value("${function}")
  private String function;

  @Value("${interval}")
  private String interval;


  @Autowired
  public BasicDaoImpl(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
    Properties properties = new Properties();
    String tsPrecision = properties.getProperty("timestamp_precision", "ms");
    switch (tsPrecision) {
      case "us":
        TIMESTAMP_RADIX = 1000;
        break;
      case "ns":
        TIMESTAMP_RADIX = 1000_000;
        break;
      default:
        TIMESTAMP_RADIX = 1;
    }
    logger.info("Use timestamp precision {}", tsPrecision);
  }

  @Override
  public List<String> getMetaData() {
    ConnectionCallback<Object> connectionCallback = new ConnectionCallback<Object>() {
      public Object doInConnection(Connection connection) throws SQLException {
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        ResultSet resultSet = databaseMetaData.getColumns(Constant.CATALOG_TIMESERIES, "root.*", "root.*", null);
        logger.info("Start to get timeseries");
        List<String> columnsName = new ArrayList<>();
        while (resultSet.next()) {
          String timeseries = resultSet.getString(1);
          columnsName.add(timeseries.substring(5));
        }
        return columnsName;
      }
    };
    return (List<String>) jdbcTemplate.execute(connectionCallback);
  }

  /**
   * Note: If the query fails this could be due to AGGREGATIION like AVG on booleayn field.
   * Thus, we then do a retry with FIRST aggregation.
   * This should be solved better in the long run.
   */
  @Override
  public List<TimeValues> querySeries(String s, Pair<ZonedDateTime, ZonedDateTime> timeRange) {
    try {
      return querySeriesInternal(s, timeRange, function);
    } catch (Exception e) {
      logger.info("Execution failed, trying now with FIRST Function!");
      // Try it with FIRST
      try {
        return querySeriesInternal(s, timeRange, FALLBACK_AGG_FUNCTION);
      } catch (Exception e2) {
        logger.warn("Even FIRST query did not succeed, returning NULL now", e2);
        return Collections.emptyList();
      }
    }
  }

  public List<TimeValues> querySeriesInternal(String s, Pair<ZonedDateTime, ZonedDateTime> timeRange, String function) {
    Long from = zonedCovertToLong(timeRange.left);
    Long to = zonedCovertToLong(timeRange.right);
    final long hours = Duration.between(timeRange.left, timeRange.right).toHours();
    List<TimeValues> rows = null;
    String sql = String.format("SELECT %s FROM root.%s WHERE time > %d and time < %d",
        s.substring(s.lastIndexOf('.') + 1), s.substring(0, s.lastIndexOf('.')),
        from * TIMESTAMP_RADIX, to * TIMESTAMP_RADIX);
    String columnName = "root." + s;
    if (isDownSampling && (hours > 1)) {
      if (hours < 30 * 24 && hours > 24) {
        interval = "1h";
      } else if (hours > 30 * 24) {
        interval = "1d";
      }
      sql = String.format(
          "SELECT " + function
              + "(%s) FROM root.%s WHERE time > %d and time < %d group by (%s, [%d, %d])",
          s.substring(s.lastIndexOf('.') + 1), s.substring(0, s.lastIndexOf('.')), from, to,
          interval, from, to);
      columnName = function + "(root." + s + ")";
    }
    logger.info(sql);
    try {
      rows = jdbcTemplate.query(sql, new TimeValuesRowMapper(columnName));
    } catch (Exception e) {
      logger.error(e.getMessage());
      throw new RuntimeException("Query did not sucees", e);
    }
    return rows;
  }

  private Long zonedCovertToLong(ZonedDateTime time) {
    return time.toInstant().toEpochMilli();
  }

  static class TimeValuesRowMapper implements RowMapper<TimeValues> {

    static final String TRUE_STR = "true";
    static final String FALSE_STR = "false";
    String columnName;

    TimeValuesRowMapper(String columnName) {
      this.columnName = columnName;
    }

    @Override
    public TimeValues mapRow(ResultSet resultSet, int i) throws SQLException {
      TimeValues tv = new TimeValues();
      tv.setTime(resultSet.getLong("Time"));
      String valueString = resultSet.getString(columnName);
      if (valueString != null) {
        if (TRUE_STR.equalsIgnoreCase(valueString)) {
          tv.setValue(1);
        } else if (FALSE_STR.equalsIgnoreCase(valueString)) {
          tv.setValue(0);
        } else {
          try {
            tv.setValue(resultSet.getFloat(columnName));
          } catch (Exception e) {
            tv.setValue(0);
          }
        }
      }
      return tv;
    }
  }

}
