package cn.edu.thu.tsfiledb.tool;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.thrift.TException;
import org.joda.time.DateTimeZone;

import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import cn.edu.thu.tsfiledb.jdbc.TsfileConnection;
import cn.edu.thu.tsfiledb.jdbc.TsfileSQLException;
import jline.console.ConsoleReader;

public abstract class AbstractCsvTool {
    protected static final String HOST_ARGS = "h";
    protected static final String HOST_NAME = "host";

    protected static final String HELP_ARGS = "help";

    protected static final String PORT_ARGS = "p";
    protected static final String PORT_NAME = "port";

    protected static final String PASSWORD_ARGS = "pw";
    protected static final String PASSWORD_NAME = "password";

    protected static final String USERNAME_ARGS = "u";
    protected static final String USERNAME_NAME = "username";
    
    protected static final String TIME_FORMAT_ARGS = "tf";
    protected static final String TIME_FORMAT_NAME = "timeformat";
	
    protected static final String TIME_ZONE_ARGS = "tz";
    protected static final String TIME_ZONE_NAME = "timeZone";
    
    protected static String host;
    protected static String port;
    protected static String username;
    protected static String password;
    
    protected static final int MAX_HELP_CONSOLE_WIDTH = 92;
    
    protected static DateTimeZone timeZone;
    protected static String timeZoneID;
    protected static String timeFormat;
    
    protected static final String JDBC_DRIVER = "cn.edu.thu.tsfiledb.jdbc.TsfileDriver";

    protected static final String DEFAULT_TIME_FORMAT = "ISO8601";

    protected static final String[] SUPPORT_TIME_FORMAT = new String[]{
			DEFAULT_TIME_FORMAT, "default",
			"long", "number", "timestamp",
			"yyyy-MM-dd HH:mm:ss",
			"yyyy/MM/dd HH:mm:ss",
			"yyyy.MM.dd HH:mm:ss",
			"yyyy-MM-dd'T'HH:mm:ss",
			"yyyy/MM/dd'T'HH:mm:ss",
			"yyyy.MM.dd'T'HH:mm:ss",
			"yyyy-MM-dd HH:mm:ssZZ",
			"yyyy/MM/dd HH:mm:ssZZ",
			"yyyy.MM.dd HH:mm:ssZZ",
			"yyyy-MM-dd'T'HH:mm:ssZZ",
			"yyyy/MM/dd'T'HH:mm:ssZZ",
			"yyyy.MM.dd'T'HH:mm:ssZZ",
			"yyyy/MM/dd HH:mm:ss.SSS",
			"yyyy-MM-dd HH:mm:ss.SSS",
			"yyyy.MM.dd HH:mm:ss.SSS",
			"yyyy/MM/dd'T'HH:mm:ss.SSS",
			"yyyy-MM-dd'T'HH:mm:ss.SSS",
			"yyyy-MM-dd'T'HH:mm:ss.SSS",
			"yyyy.MM.dd'T'HH:mm:ss.SSS",
			"yyyy-MM-dd HH:mm:ss.SSSZZ",
			"yyyy/MM/dd HH:mm:ss.SSSZZ",
			"yyyy.MM.dd HH:mm:ss.SSSZZ",
			"yyyy-MM-dd'T'HH:mm:ss.SSSZZ",
			"yyyy/MM/dd'T'HH:mm:ss.SSSZZ",
	};
    
    protected static TsfileConnection connection;

	protected static String checkRequiredArg(String arg, String name, CommandLine commandLine) throws ArgsErrorException {
		String str = commandLine.getOptionValue(arg);
		if (str == null) {
			String msg = String.format("Required values for option '%s' not provided", name);
			System.out.println(msg);
			System.out.println("Use -help for more information");
			throw new ArgsErrorException(msg);
		}
		return str;
	}
    
	protected static void setTimeZone() throws TsfileSQLException, TException {
		if(timeZoneID != null){
			connection.setTimeZone(timeZoneID);
		}
		timeZone = DateTimeZone.forID(connection.getTimeZone());
	}
	
	protected static void parseBasicParams(CommandLine commandLine, ConsoleReader reader) throws ArgsErrorException, IOException {
		host = checkRequiredArg(HOST_ARGS, HOST_NAME, commandLine);
		port = checkRequiredArg(PORT_ARGS, PORT_NAME, commandLine);
		username = checkRequiredArg(USERNAME_ARGS, USERNAME_NAME, commandLine);

		password = commandLine.getOptionValue(PASSWORD_ARGS);
		if (password == null) {
			password = reader.readLine("please input your password:", '\0');
		}
	}
	
	protected static boolean checkTimeFormat(){
		for(String format : SUPPORT_TIME_FORMAT){
			if(timeFormat.equals(format)){
				return true;
			}
		}
		System.out.println(String.format("Input time format %s is not supported, "
				+ "please input like yyyy-MM-dd\\ HH:mm:ss.SSS or yyyy-MM-dd'T'HH:mm:ss.SSS", timeFormat));
		return false;
	}
}
