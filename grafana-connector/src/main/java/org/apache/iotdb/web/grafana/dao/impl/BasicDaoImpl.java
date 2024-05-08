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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
    return jdbcTemplate.execute(
        (ConnectionCallback<List<String>>)
            connection -> {
              try (Statement statement = connection.createStatement()) {
                statement.execute("show timeseries root.**");
                logger.info("Start to get timeseries");
                try (ResultSet resultSet = statement.getResultSet()) {
                  List<String> columnsName = new ArrayList<>();
                  while (resultSet.next()) {
                    columnsName.add(resultSet.getString(1).substring(5));
                  }
                  return columnsName;
                }
              }
            });
  }

  public static void setTimestampRadioX(String timestampPrecision) {
    switch (timestampPrecision) {
      case "us":
        timestampRadioX = 1000L;
        break;
      case "ns":
        timestampRadioX = 1000_000L;
        break;
      default:
        timestampRadioX = 1L;
    }
    logger.info("Use timestamp precision {}", timestampPrecision.replaceAll("[\n\r\t]", "_"));
  }

  /**
   * Note: If the query fails this could be due to AGGREGATION like AVG on boolean field. Thus, we
   * then do a retry with FIRST aggregation. This should be solved better in the long run.
   */
  @Override
  public List<TimeValues> querySeries(String s, Pair<ZonedDateTime, ZonedDateTime> timeRange) {
    if (timestampRadioX == -1L) {
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
    long from = zonedCovertToLong(timeRange.left);
    long to = zonedCovertToLong(timeRange.right);
    final long hours = Duration.between(timeRange.left, timeRange.right).toHours();

    String sql = "SELECT ? FROM root.? WHERE time > ? and time < ?";
    Object[] params =
        new Object[] {
          s.substring(s.lastIndexOf('.') + 1),
          s.substring(0, s.lastIndexOf('.')),
          from * timestampRadioX,
          to * timestampRadioX,
        };
    String columnName = "root." + s;

    String intervalLocal = getInterval(hours);
    if (!"".equals(intervalLocal)) {
      sql = "SELECT ?(?) FROM root.? WHERE time > ? and time < ? group by ([?, ?),?)";
      params =
          new Object[] {
            function,
            s.substring(s.lastIndexOf('.') + 1),
            s.substring(0, s.lastIndexOf('.')),
            from * timestampRadioX,
            to * timestampRadioX,
            from * timestampRadioX,
            to * timestampRadioX,
            intervalLocal
          };
      columnName = function + "(root." + s + ")";
    }

    logger.info("SQL: {}, Params: {}", sql, params);
    return jdbcTemplate.query(sql, params, new TimeValuesRowMapper(columnName));
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

  private long zonedCovertToLong(ZonedDateTime time) {
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
