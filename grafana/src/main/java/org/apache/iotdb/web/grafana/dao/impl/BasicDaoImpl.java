package org.apache.iotdb.web.grafana.dao.impl;

import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.web.grafana.bean.TimeValues;
import org.apache.iotdb.web.grafana.dao.BasicDao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dell on 2017/7/17.
 */
@Repository
public class BasicDaoImpl implements BasicDao {

    private static final Logger logger = LoggerFactory.getLogger(BasicDaoImpl.class);

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public BasicDaoImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public List<String> getMetaData() {
		ConnectionCallback<Object> connectionCallback = new ConnectionCallback<Object>() {
			public Object doInConnection(Connection connection) throws SQLException {
				DatabaseMetaData databaseMetaData = connection.getMetaData();
				ResultSet resultSet = databaseMetaData.getColumns(null, null, "root.*", null);
				System.out.println("Start to get timeseries");
				List<String> columnsName = new ArrayList<>();
				while (resultSet.next()) {
					String timeseries = resultSet.getString(1);
					columnsName.add(timeseries.substring(5));
				}
				return columnsName;
			}
        };
        return (List<String>)jdbcTemplate.execute(connectionCallback);
    }

    @Override
    public List<TimeValues> querySeries(String s, Pair<ZonedDateTime, ZonedDateTime> timeRange) {
        Long from = zonedCovertToLong(timeRange.left);
        Long to = zonedCovertToLong(timeRange.right);
        String sql = "SELECT " + s.substring(s.lastIndexOf('.')+1) + " FROM root." + s.substring(0, s.lastIndexOf('.')) + " WHERE time > " + from + " and time < " + to;
        logger.info(sql);
        List<TimeValues> rows = null;
		try {
			rows = jdbcTemplate.query(sql, new TimeValuesRowMapper("root." + s));
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
        return rows;
    }

    private Long zonedCovertToLong(ZonedDateTime time) {
        return time.toInstant().toEpochMilli();
    }

	static class TimeValuesRowMapper implements RowMapper<TimeValues> {
		String columnName;
		final String TRUE_STR = "true";
		final String FALSE_STR = "false";

		TimeValuesRowMapper(String columnName) {
			this.columnName = columnName;
		}

		@Override
		public TimeValues mapRow(ResultSet resultSet, int i) throws SQLException {
			TimeValues tv = new TimeValues();
			tv.setTime(resultSet.getLong("Time"));
			String vString = resultSet.getString(columnName);
			if (vString != null) {
				if (TRUE_STR.equals(vString.toLowerCase())) {
					tv.setValue(1);
				} else if (FALSE_STR.equals(vString.toLowerCase())) {
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

