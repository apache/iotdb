package cn.edu.thu.tsfiledb.auth;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

public class Test {


	public static void main(String[] args) {
		DateTime dateTime = new DateTime("1970-01-01T01:00:00.001", DateTimeZone.forID("+01:00"));
		System.out.println(dateTime);
//		System.out.println(dateTime.toDateTime(DateTimeZone.forID("+01:00")));

	}
}
