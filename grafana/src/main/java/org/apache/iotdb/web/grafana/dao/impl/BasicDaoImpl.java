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
import java.sql.Statement;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Created by dell on 2017/7/17. */
@Repository
@PropertySource("classpath:application.properties")
public class BasicDaoImpl implements BasicDao {

  private static final Logger logger = LoggerFactory.getLogger(BasicDaoImpl.class);

  private final JdbcTemplate jdbcTemplate;

  private static long timestampRadioX = -1L;

  @Value("${timestamp_precision}")
  private String timestampPrecision = "ms";

  @Value("${isDownSampling}")
  private boolean isDownSampling;

  @Value("${continuous_data_function}")
  private String continuousDataFunction;

  @Value("${discrete_data_function}")
  private String discreteDataFunction;

  @Value("${interval}")
  private String interval;

  @Autowired
  public BasicDaoImpl(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Override
  public List<String> getMetaData() {
    ConnectionCallback<Object> connectionCallback =
        new ConnectionCallback<Object>() {
          @Override
          public Object doInConnection(Connection connection) throws SQLException {
            try (Statement statement = connection.createStatement()) {
              statement.execute("show timeseries root.*");
              try (ResultSet resultSet = statement.getResultSet()) {
                logger.info("Start to get timeseries");
                List<String> columnsName = new ArrayList<>();
                while (resultSet.next()) {
                  String timeseries = resultSet.getString(1);
                  columnsName.add(timeseries.substring(5));
                }
                return columnsName;
              }
            }
          }
        };
    return (List<String>) jdbcTemplate.execute(connectionCallback);
  }

  public static void setTimestampRadioX(String timestampPrecision) {
    switch (timestampPrecision) {
      case "us":
        timestampRadioX = 1000;
        break;
      case "ns":
        timestampRadioX = 1000_000;
        break;
      default:
        timestampRadioX = 1;
    }
    logger.info("Use timestamp precision {}", timestampPrecision);
  }

  /**
   * Note: If the query fails this could be due to AGGREGATIION like AVG on booleayn field. Thus, we
   * then do a retry with FIRST aggregation. This should be solved better in the long run.
   */
  @Override
  public List<TimeValues> querySeries(String s, Pair<ZonedDateTime, ZonedDateTime> timeRange) {
    if (timestampRadioX == -1) {
      setTimestampRadioX(timestampPrecision);
    }
    try {
      return querySeriesInternal(s, timeRange, continuousDataFunction);
    } catch (Exception e) {
      // Try it with discreteDataFunction
      try {
        return querySeriesInternal(s, timeRange, discreteDataFunction);
      } catch (Exception e2) {
        logger.warn("Even {} query did not succeed, returning NULL now", discreteDataFunction, e2);
        return Collections.emptyList();
      }
    }
  }

  public List<TimeValues> querySeriesInternal(
      String s, Pair<ZonedDateTime, ZonedDateTime> timeRange, String function) {
    Long from = zonedCovertToLong(timeRange.left);
    Long to = zonedCovertToLong(timeRange.right);
    final long hours = Duration.between(timeRange.left, timeRange.right).toHours();

    String sql =
        String.format(
            "SELECT %s FROM root.%s WHERE time > %d and time < %d",
            s.substring(s.lastIndexOf('.') + 1),
            s.substring(0, s.lastIndexOf('.')),
            from * timestampRadioX,
            to * timestampRadioX);
    String columnName = "root." + s;

    String intervalLocal = getInterval(hours);
    if (!intervalLocal.equals("")) {
      sql =
          String.format(
              "SELECT "
                  + function
                  + "(%s) FROM root.%s WHERE time > %d and time < %d group by ([%d, %d),%s)",
              s.substring(s.lastIndexOf('.') + 1),
              s.substring(0, s.lastIndexOf('.')),
              from * timestampRadioX,
              to * timestampRadioX,
              from * timestampRadioX,
              to * timestampRadioX,
              intervalLocal);
      columnName = function + "(root." + s + ")";
    }

    logger.info(sql);
    return jdbcTemplate.query(sql, new TimeValuesRowMapper(columnName));
  }

  public String getInterval(final long hours) {
    if (!isDownSampling || !(hours > 1)) {
      return "";
    }

    if (hours < 30 * 24 && hours > 24) {
      return "1h";
    } else if (hours > 30 * 24) {
      return "1d";
    }

    return this.interval;
  }

  private Long zonedCovertToLong(ZonedDateTime time) {
    return time.toInstant().toEpochMilli();
  }

  static class TimeValuesRowMapper implements RowMapper<TimeValues> {

    String columnName;

    TimeValuesRowMapper(String columnName) {
      this.columnName = columnName;
    }

    @Override
    public TimeValues mapRow(ResultSet resultSet, int i) throws SQLException {
      TimeValues tv = new TimeValues();
      tv.setTime(resultSet.getLong("Time") / timestampRadioX);
      String valueString = resultSet.getString(columnName);
      if (valueString != null) {
        try {
          tv.setValue(Float.parseFloat(resultSet.getString(columnName)));
        } catch (Exception e) {
          tv.setValue(resultSet.getString(columnName));
        }
      }
      return tv;
    }
  }
}
