// $ANTLR 3.5.2 TSLexer.g 2017-07-23 22:47:20

package cn.edu.thu.tsfiledb.sql.parse;



import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

@SuppressWarnings("all")
public class TSLexer extends Lexer {
	public static final int EOF=-1;
	public static final int COLON=4;
	public static final int COMMA=5;
	public static final int DATETIME=6;
	public static final int DIVIDE=7;
	public static final int DOT=8;
	public static final int Digit=9;
	public static final int EQUAL=10;
	public static final int EQUAL_NS=11;
	public static final int Float=12;
	public static final int GREATERTHAN=13;
	public static final int GREATERTHANOREQUALTO=14;
	public static final int HexDigit=15;
	public static final int Identifier=16;
	public static final int Integer=17;
	public static final int KW_ADD=18;
	public static final int KW_AND=19;
	public static final int KW_BY=20;
	public static final int KW_CREATE=21;
	public static final int KW_DATATYPE=22;
	public static final int KW_DELETE=23;
	public static final int KW_DESCRIBE=24;
	public static final int KW_DROP=25;
	public static final int KW_ENCODING=26;
	public static final int KW_FROM=27;
	public static final int KW_GRANT=28;
	public static final int KW_GROUP=29;
	public static final int KW_INSERT=30;
	public static final int KW_INTO=31;
	public static final int KW_LABEL=32;
	public static final int KW_LINK=33;
	public static final int KW_LOAD=34;
	public static final int KW_MERGE=35;
	public static final int KW_METADATA=36;
	public static final int KW_NOT=37;
	public static final int KW_NULL=38;
	public static final int KW_ON=39;
	public static final int KW_OR=40;
	public static final int KW_ORDER=41;
	public static final int KW_PASSWORD=42;
	public static final int KW_PRIVILEGES=43;
	public static final int KW_PROPERTY=44;
	public static final int KW_QUIT=45;
	public static final int KW_REVOKE=46;
	public static final int KW_ROLE=47;
	public static final int KW_SELECT=48;
	public static final int KW_SET=49;
	public static final int KW_SHOW=50;
	public static final int KW_STORAGE=51;
	public static final int KW_TIMESERIES=52;
	public static final int KW_TIMESTAMP=53;
	public static final int KW_TO=54;
	public static final int KW_UNLINK=55;
	public static final int KW_UPDATE=56;
	public static final int KW_USER=57;
	public static final int KW_VALUE=58;
	public static final int KW_VALUES=59;
	public static final int KW_WHERE=60;
	public static final int KW_WITH=61;
	public static final int LESSTHAN=62;
	public static final int LESSTHANOREQUALTO=63;
	public static final int LPAREN=64;
	public static final int Letter=65;
	public static final int MINUS=66;
	public static final int NOTEQUAL=67;
	public static final int PLUS=68;
	public static final int QUOTE=69;
	public static final int RPAREN=70;
	public static final int SEMICOLON=71;
	public static final int STAR=72;
	public static final int StringLiteral=73;
	public static final int WS=74;

	// delegates
	// delegators
	public Lexer[] getDelegates() {
		return new Lexer[] {};
	}

	public TSLexer() {} 
	public TSLexer(CharStream input) {
		this(input, new RecognizerSharedState());
	}
	public TSLexer(CharStream input, RecognizerSharedState state) {
		super(input,state);
	}
	@Override public String getGrammarFileName() { return "TSLexer.g"; }

	// $ANTLR start "KW_PRIVILEGES"
	public final void mKW_PRIVILEGES() throws RecognitionException {
		try {
			int _type = KW_PRIVILEGES;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:10:15: ( 'PRIVILEGES' )
			// TSLexer.g:10:17: 'PRIVILEGES'
			{
			match("PRIVILEGES"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_PRIVILEGES"

	// $ANTLR start "KW_TIMESERIES"
	public final void mKW_TIMESERIES() throws RecognitionException {
		try {
			int _type = KW_TIMESERIES;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:11:15: ( 'TIMESERIES' )
			// TSLexer.g:11:17: 'TIMESERIES'
			{
			match("TIMESERIES"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_TIMESERIES"

	// $ANTLR start "KW_ROLE"
	public final void mKW_ROLE() throws RecognitionException {
		try {
			int _type = KW_ROLE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:12:9: ( 'ROLE' )
			// TSLexer.g:12:11: 'ROLE'
			{
			match("ROLE"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_ROLE"

	// $ANTLR start "KW_GRANT"
	public final void mKW_GRANT() throws RecognitionException {
		try {
			int _type = KW_GRANT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:13:9: ( 'GRANT' )
			// TSLexer.g:13:11: 'GRANT'
			{
			match("GRANT"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_GRANT"

	// $ANTLR start "KW_REVOKE"
	public final void mKW_REVOKE() throws RecognitionException {
		try {
			int _type = KW_REVOKE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:14:10: ( 'REVOKE' )
			// TSLexer.g:14:12: 'REVOKE'
			{
			match("REVOKE"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_REVOKE"

	// $ANTLR start "KW_MERGE"
	public final void mKW_MERGE() throws RecognitionException {
		try {
			int _type = KW_MERGE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:15:9: ( 'MERGE' )
			// TSLexer.g:15:11: 'MERGE'
			{
			match("MERGE"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_MERGE"

	// $ANTLR start "KW_QUIT"
	public final void mKW_QUIT() throws RecognitionException {
		try {
			int _type = KW_QUIT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:16:8: ( 'QUIT' )
			// TSLexer.g:16:10: 'QUIT'
			{
			match("QUIT"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_QUIT"

	// $ANTLR start "KW_METADATA"
	public final void mKW_METADATA() throws RecognitionException {
		try {
			int _type = KW_METADATA;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:17:12: ( 'METADATA' )
			// TSLexer.g:17:14: 'METADATA'
			{
			match("METADATA"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_METADATA"

	// $ANTLR start "KW_DATATYPE"
	public final void mKW_DATATYPE() throws RecognitionException {
		try {
			int _type = KW_DATATYPE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:18:12: ( 'DATATYPE' )
			// TSLexer.g:18:14: 'DATATYPE'
			{
			match("DATATYPE"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_DATATYPE"

	// $ANTLR start "KW_ENCODING"
	public final void mKW_ENCODING() throws RecognitionException {
		try {
			int _type = KW_ENCODING;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:19:12: ( 'ENCODING' )
			// TSLexer.g:19:14: 'ENCODING'
			{
			match("ENCODING"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_ENCODING"

	// $ANTLR start "KW_STORAGE"
	public final void mKW_STORAGE() throws RecognitionException {
		try {
			int _type = KW_STORAGE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:21:11: ( 'STORAGE' )
			// TSLexer.g:21:13: 'STORAGE'
			{
			match("STORAGE"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_STORAGE"

	// $ANTLR start "KW_AND"
	public final void mKW_AND() throws RecognitionException {
		try {
			int _type = KW_AND;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:23:8: ( 'AND' )
			// TSLexer.g:23:10: 'AND'
			{
			match("AND"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_AND"

	// $ANTLR start "KW_OR"
	public final void mKW_OR() throws RecognitionException {
		try {
			int _type = KW_OR;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:24:7: ( 'OR' )
			// TSLexer.g:24:9: 'OR'
			{
			match("OR"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_OR"

	// $ANTLR start "KW_NOT"
	public final void mKW_NOT() throws RecognitionException {
		try {
			int _type = KW_NOT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:25:8: ( 'NOT' | '!' )
			int alt1=2;
			int LA1_0 = input.LA(1);
			if ( (LA1_0=='N') ) {
				alt1=1;
			}
			else if ( (LA1_0=='!') ) {
				alt1=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 1, 0, input);
				throw nvae;
			}

			switch (alt1) {
				case 1 :
					// TSLexer.g:25:10: 'NOT'
					{
					match("NOT"); 

					}
					break;
				case 2 :
					// TSLexer.g:25:18: '!'
					{
					match('!'); 
					}
					break;

			}
			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_NOT"

	// $ANTLR start "KW_ORDER"
	public final void mKW_ORDER() throws RecognitionException {
		try {
			int _type = KW_ORDER;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:28:10: ( 'ORDER' )
			// TSLexer.g:28:12: 'ORDER'
			{
			match("ORDER"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_ORDER"

	// $ANTLR start "KW_GROUP"
	public final void mKW_GROUP() throws RecognitionException {
		try {
			int _type = KW_GROUP;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:29:10: ( 'GROUP' )
			// TSLexer.g:29:12: 'GROUP'
			{
			match("GROUP"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_GROUP"

	// $ANTLR start "KW_BY"
	public final void mKW_BY() throws RecognitionException {
		try {
			int _type = KW_BY;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:30:7: ( 'BY' )
			// TSLexer.g:30:9: 'BY'
			{
			match("BY"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_BY"

	// $ANTLR start "KW_WHERE"
	public final void mKW_WHERE() throws RecognitionException {
		try {
			int _type = KW_WHERE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:32:10: ( 'WHERE' )
			// TSLexer.g:32:12: 'WHERE'
			{
			match("WHERE"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_WHERE"

	// $ANTLR start "KW_FROM"
	public final void mKW_FROM() throws RecognitionException {
		try {
			int _type = KW_FROM;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:33:9: ( 'FROM' )
			// TSLexer.g:33:11: 'FROM'
			{
			match("FROM"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_FROM"

	// $ANTLR start "KW_SELECT"
	public final void mKW_SELECT() throws RecognitionException {
		try {
			int _type = KW_SELECT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:34:11: ( 'SELECT' )
			// TSLexer.g:34:13: 'SELECT'
			{
			match("SELECT"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_SELECT"

	// $ANTLR start "KW_INSERT"
	public final void mKW_INSERT() throws RecognitionException {
		try {
			int _type = KW_INSERT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:36:11: ( 'INSERT' )
			// TSLexer.g:36:13: 'INSERT'
			{
			match("INSERT"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_INSERT"

	// $ANTLR start "KW_ON"
	public final void mKW_ON() throws RecognitionException {
		try {
			int _type = KW_ON;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:37:7: ( 'ON' )
			// TSLexer.g:37:9: 'ON'
			{
			match("ON"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_ON"

	// $ANTLR start "KW_SHOW"
	public final void mKW_SHOW() throws RecognitionException {
		try {
			int _type = KW_SHOW;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:38:8: ( 'SHOW' )
			// TSLexer.g:38:10: 'SHOW'
			{
			match("SHOW"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_SHOW"

	// $ANTLR start "KW_LOAD"
	public final void mKW_LOAD() throws RecognitionException {
		try {
			int _type = KW_LOAD;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:40:8: ( 'LOAD' )
			// TSLexer.g:40:10: 'LOAD'
			{
			match("LOAD"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_LOAD"

	// $ANTLR start "KW_NULL"
	public final void mKW_NULL() throws RecognitionException {
		try {
			int _type = KW_NULL;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:42:8: ( 'NULL' )
			// TSLexer.g:42:10: 'NULL'
			{
			match("NULL"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_NULL"

	// $ANTLR start "KW_CREATE"
	public final void mKW_CREATE() throws RecognitionException {
		try {
			int _type = KW_CREATE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:43:10: ( 'CREATE' )
			// TSLexer.g:43:12: 'CREATE'
			{
			match("CREATE"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_CREATE"

	// $ANTLR start "KW_DROP"
	public final void mKW_DROP() throws RecognitionException {
		try {
			int _type = KW_DROP;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:45:8: ( 'DROP' )
			// TSLexer.g:45:10: 'DROP'
			{
			match("DROP"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_DROP"

	// $ANTLR start "KW_TO"
	public final void mKW_TO() throws RecognitionException {
		try {
			int _type = KW_TO;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:46:6: ( 'TO' )
			// TSLexer.g:46:8: 'TO'
			{
			match("TO"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_TO"

	// $ANTLR start "KW_TIMESTAMP"
	public final void mKW_TIMESTAMP() throws RecognitionException {
		try {
			int _type = KW_TIMESTAMP;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:48:13: ( 'TIMESTAMP' )
			// TSLexer.g:48:15: 'TIMESTAMP'
			{
			match("TIMESTAMP"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_TIMESTAMP"

	// $ANTLR start "KW_USER"
	public final void mKW_USER() throws RecognitionException {
		try {
			int _type = KW_USER;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:49:8: ( 'USER' )
			// TSLexer.g:49:10: 'USER'
			{
			match("USER"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_USER"

	// $ANTLR start "KW_INTO"
	public final void mKW_INTO() throws RecognitionException {
		try {
			int _type = KW_INTO;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:50:8: ( 'INTO' )
			// TSLexer.g:50:10: 'INTO'
			{
			match("INTO"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_INTO"

	// $ANTLR start "KW_WITH"
	public final void mKW_WITH() throws RecognitionException {
		try {
			int _type = KW_WITH;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:51:8: ( 'WITH' )
			// TSLexer.g:51:10: 'WITH'
			{
			match("WITH"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_WITH"

	// $ANTLR start "KW_SET"
	public final void mKW_SET() throws RecognitionException {
		try {
			int _type = KW_SET;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:52:7: ( 'SET' )
			// TSLexer.g:52:9: 'SET'
			{
			match("SET"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_SET"

	// $ANTLR start "KW_DELETE"
	public final void mKW_DELETE() throws RecognitionException {
		try {
			int _type = KW_DELETE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:53:10: ( 'DELETE' )
			// TSLexer.g:53:12: 'DELETE'
			{
			match("DELETE"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_DELETE"

	// $ANTLR start "KW_UPDATE"
	public final void mKW_UPDATE() throws RecognitionException {
		try {
			int _type = KW_UPDATE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:54:10: ( 'UPDATE' )
			// TSLexer.g:54:12: 'UPDATE'
			{
			match("UPDATE"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_UPDATE"

	// $ANTLR start "KW_VALUES"
	public final void mKW_VALUES() throws RecognitionException {
		try {
			int _type = KW_VALUES;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:55:10: ( 'VALUES' )
			// TSLexer.g:55:12: 'VALUES'
			{
			match("VALUES"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_VALUES"

	// $ANTLR start "KW_VALUE"
	public final void mKW_VALUE() throws RecognitionException {
		try {
			int _type = KW_VALUE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:56:9: ( 'VALUE' )
			// TSLexer.g:56:11: 'VALUE'
			{
			match("VALUE"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_VALUE"

	// $ANTLR start "KW_PASSWORD"
	public final void mKW_PASSWORD() throws RecognitionException {
		try {
			int _type = KW_PASSWORD;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:57:12: ( 'PASSWORD' )
			// TSLexer.g:57:14: 'PASSWORD'
			{
			match("PASSWORD"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_PASSWORD"

	// $ANTLR start "KW_DESCRIBE"
	public final void mKW_DESCRIBE() throws RecognitionException {
		try {
			int _type = KW_DESCRIBE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:58:12: ( 'DESCRIBE' )
			// TSLexer.g:58:14: 'DESCRIBE'
			{
			match("DESCRIBE"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_DESCRIBE"

	// $ANTLR start "KW_PROPERTY"
	public final void mKW_PROPERTY() throws RecognitionException {
		try {
			int _type = KW_PROPERTY;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:59:12: ( 'PROPERTY' )
			// TSLexer.g:59:14: 'PROPERTY'
			{
			match("PROPERTY"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_PROPERTY"

	// $ANTLR start "KW_ADD"
	public final void mKW_ADD() throws RecognitionException {
		try {
			int _type = KW_ADD;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:60:7: ( 'ADD' )
			// TSLexer.g:60:9: 'ADD'
			{
			match("ADD"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_ADD"

	// $ANTLR start "KW_LABEL"
	public final void mKW_LABEL() throws RecognitionException {
		try {
			int _type = KW_LABEL;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:61:9: ( 'LABEL' )
			// TSLexer.g:61:11: 'LABEL'
			{
			match("LABEL"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_LABEL"

	// $ANTLR start "KW_LINK"
	public final void mKW_LINK() throws RecognitionException {
		try {
			int _type = KW_LINK;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:62:8: ( 'LINK' )
			// TSLexer.g:62:10: 'LINK'
			{
			match("LINK"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_LINK"

	// $ANTLR start "KW_UNLINK"
	public final void mKW_UNLINK() throws RecognitionException {
		try {
			int _type = KW_UNLINK;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:63:10: ( 'UNLINK' )
			// TSLexer.g:63:12: 'UNLINK'
			{
			match("UNLINK"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "KW_UNLINK"

	// $ANTLR start "QUOTE"
	public final void mQUOTE() throws RecognitionException {
		try {
			int _type = QUOTE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:66:7: ( '\\'' )
			// TSLexer.g:66:9: '\\''
			{
			match('\''); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "QUOTE"

	// $ANTLR start "DOT"
	public final void mDOT() throws RecognitionException {
		try {
			int _type = DOT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:68:5: ( '.' )
			// TSLexer.g:68:7: '.'
			{
			match('.'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "DOT"

	// $ANTLR start "COLON"
	public final void mCOLON() throws RecognitionException {
		try {
			int _type = COLON;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:69:7: ( ':' )
			// TSLexer.g:69:9: ':'
			{
			match(':'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "COLON"

	// $ANTLR start "COMMA"
	public final void mCOMMA() throws RecognitionException {
		try {
			int _type = COMMA;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:70:7: ( ',' )
			// TSLexer.g:70:9: ','
			{
			match(','); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "COMMA"

	// $ANTLR start "SEMICOLON"
	public final void mSEMICOLON() throws RecognitionException {
		try {
			int _type = SEMICOLON;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:71:11: ( ';' )
			// TSLexer.g:71:13: ';'
			{
			match(';'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "SEMICOLON"

	// $ANTLR start "LPAREN"
	public final void mLPAREN() throws RecognitionException {
		try {
			int _type = LPAREN;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:73:8: ( '(' )
			// TSLexer.g:73:10: '('
			{
			match('('); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "LPAREN"

	// $ANTLR start "RPAREN"
	public final void mRPAREN() throws RecognitionException {
		try {
			int _type = RPAREN;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:74:8: ( ')' )
			// TSLexer.g:74:10: ')'
			{
			match(')'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "RPAREN"

	// $ANTLR start "EQUAL"
	public final void mEQUAL() throws RecognitionException {
		try {
			int _type = EQUAL;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:76:7: ( '=' | '==' )
			int alt2=2;
			int LA2_0 = input.LA(1);
			if ( (LA2_0=='=') ) {
				int LA2_1 = input.LA(2);
				if ( (LA2_1=='=') ) {
					alt2=2;
				}

				else {
					alt2=1;
				}

			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 2, 0, input);
				throw nvae;
			}

			switch (alt2) {
				case 1 :
					// TSLexer.g:76:9: '='
					{
					match('='); 
					}
					break;
				case 2 :
					// TSLexer.g:76:15: '=='
					{
					match("=="); 

					}
					break;

			}
			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "EQUAL"

	// $ANTLR start "EQUAL_NS"
	public final void mEQUAL_NS() throws RecognitionException {
		try {
			int _type = EQUAL_NS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:77:10: ( '<=>' )
			// TSLexer.g:77:12: '<=>'
			{
			match("<=>"); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "EQUAL_NS"

	// $ANTLR start "NOTEQUAL"
	public final void mNOTEQUAL() throws RecognitionException {
		try {
			int _type = NOTEQUAL;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:78:10: ( '<>' | '!=' )
			int alt3=2;
			int LA3_0 = input.LA(1);
			if ( (LA3_0=='<') ) {
				alt3=1;
			}
			else if ( (LA3_0=='!') ) {
				alt3=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 3, 0, input);
				throw nvae;
			}

			switch (alt3) {
				case 1 :
					// TSLexer.g:78:12: '<>'
					{
					match("<>"); 

					}
					break;
				case 2 :
					// TSLexer.g:78:19: '!='
					{
					match("!="); 

					}
					break;

			}
			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "NOTEQUAL"

	// $ANTLR start "LESSTHANOREQUALTO"
	public final void mLESSTHANOREQUALTO() throws RecognitionException {
		try {
			int _type = LESSTHANOREQUALTO;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:79:19: ( '<=' )
			// TSLexer.g:79:21: '<='
			{
			match("<="); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "LESSTHANOREQUALTO"

	// $ANTLR start "LESSTHAN"
	public final void mLESSTHAN() throws RecognitionException {
		try {
			int _type = LESSTHAN;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:80:10: ( '<' )
			// TSLexer.g:80:12: '<'
			{
			match('<'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "LESSTHAN"

	// $ANTLR start "GREATERTHANOREQUALTO"
	public final void mGREATERTHANOREQUALTO() throws RecognitionException {
		try {
			int _type = GREATERTHANOREQUALTO;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:81:22: ( '>=' )
			// TSLexer.g:81:24: '>='
			{
			match(">="); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "GREATERTHANOREQUALTO"

	// $ANTLR start "GREATERTHAN"
	public final void mGREATERTHAN() throws RecognitionException {
		try {
			int _type = GREATERTHAN;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:82:13: ( '>' )
			// TSLexer.g:82:15: '>'
			{
			match('>'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "GREATERTHAN"

	// $ANTLR start "DIVIDE"
	public final void mDIVIDE() throws RecognitionException {
		try {
			int _type = DIVIDE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:84:8: ( '/' )
			// TSLexer.g:84:10: '/'
			{
			match('/'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "DIVIDE"

	// $ANTLR start "PLUS"
	public final void mPLUS() throws RecognitionException {
		try {
			int _type = PLUS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:85:6: ( '+' )
			// TSLexer.g:85:8: '+'
			{
			match('+'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "PLUS"

	// $ANTLR start "MINUS"
	public final void mMINUS() throws RecognitionException {
		try {
			int _type = MINUS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:86:7: ( '-' )
			// TSLexer.g:86:9: '-'
			{
			match('-'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "MINUS"

	// $ANTLR start "STAR"
	public final void mSTAR() throws RecognitionException {
		try {
			int _type = STAR;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:87:6: ( '*' )
			// TSLexer.g:87:8: '*'
			{
			match('*'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "STAR"

	// $ANTLR start "Letter"
	public final void mLetter() throws RecognitionException {
		try {
			// TSLexer.g:92:5: ( 'a' .. 'z' | 'A' .. 'Z' )
			// TSLexer.g:
			{
			if ( (input.LA(1) >= 'A' && input.LA(1) <= 'Z')||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "Letter"

	// $ANTLR start "HexDigit"
	public final void mHexDigit() throws RecognitionException {
		try {
			// TSLexer.g:97:5: ( 'a' .. 'f' | 'A' .. 'F' )
			// TSLexer.g:
			{
			if ( (input.LA(1) >= 'A' && input.LA(1) <= 'F')||(input.LA(1) >= 'a' && input.LA(1) <= 'f') ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "HexDigit"

	// $ANTLR start "Digit"
	public final void mDigit() throws RecognitionException {
		try {
			// TSLexer.g:102:5: ( '0' .. '9' )
			// TSLexer.g:
			{
			if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "Digit"

	// $ANTLR start "StringLiteral"
	public final void mStringLiteral() throws RecognitionException {
		try {
			int _type = StringLiteral;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:107:5: ( ( '\\'' (~ ( '\\'' ) )* '\\'' | '\\\"' (~ ( '\\\"' ) )* '\\\"' ) )
			// TSLexer.g:108:5: ( '\\'' (~ ( '\\'' ) )* '\\'' | '\\\"' (~ ( '\\\"' ) )* '\\\"' )
			{
			// TSLexer.g:108:5: ( '\\'' (~ ( '\\'' ) )* '\\'' | '\\\"' (~ ( '\\\"' ) )* '\\\"' )
			int alt6=2;
			int LA6_0 = input.LA(1);
			if ( (LA6_0=='\'') ) {
				alt6=1;
			}
			else if ( (LA6_0=='\"') ) {
				alt6=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 6, 0, input);
				throw nvae;
			}

			switch (alt6) {
				case 1 :
					// TSLexer.g:108:7: '\\'' (~ ( '\\'' ) )* '\\''
					{
					match('\''); 
					// TSLexer.g:108:12: (~ ( '\\'' ) )*
					loop4:
					while (true) {
						int alt4=2;
						int LA4_0 = input.LA(1);
						if ( ((LA4_0 >= '\u0000' && LA4_0 <= '&')||(LA4_0 >= '(' && LA4_0 <= '\uFFFF')) ) {
							alt4=1;
						}

						switch (alt4) {
						case 1 :
							// TSLexer.g:
							{
							if ( (input.LA(1) >= '\u0000' && input.LA(1) <= '&')||(input.LA(1) >= '(' && input.LA(1) <= '\uFFFF') ) {
								input.consume();
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(null,input);
								recover(mse);
								throw mse;
							}
							}
							break;

						default :
							break loop4;
						}
					}

					match('\''); 
					}
					break;
				case 2 :
					// TSLexer.g:109:7: '\\\"' (~ ( '\\\"' ) )* '\\\"'
					{
					match('\"'); 
					// TSLexer.g:109:12: (~ ( '\\\"' ) )*
					loop5:
					while (true) {
						int alt5=2;
						int LA5_0 = input.LA(1);
						if ( ((LA5_0 >= '\u0000' && LA5_0 <= '!')||(LA5_0 >= '#' && LA5_0 <= '\uFFFF')) ) {
							alt5=1;
						}

						switch (alt5) {
						case 1 :
							// TSLexer.g:
							{
							if ( (input.LA(1) >= '\u0000' && input.LA(1) <= '!')||(input.LA(1) >= '#' && input.LA(1) <= '\uFFFF') ) {
								input.consume();
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(null,input);
								recover(mse);
								throw mse;
							}
							}
							break;

						default :
							break loop5;
						}
					}

					match('\"'); 
					}
					break;

			}

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "StringLiteral"

	// $ANTLR start "DATETIME"
	public final void mDATETIME() throws RecognitionException {
		try {
			int _type = DATETIME;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:115:5: ( ( Digit )+ MINUS ( Digit )+ MINUS ( Digit )+ 'T' ( Digit )+ COLON ( Digit )+ COLON ( Digit )+ ( DOT ( Digit )+ )? PLUS ( Digit )+ COLON ( Digit )+ )
			// TSLexer.g:115:7: ( Digit )+ MINUS ( Digit )+ MINUS ( Digit )+ 'T' ( Digit )+ COLON ( Digit )+ COLON ( Digit )+ ( DOT ( Digit )+ )? PLUS ( Digit )+ COLON ( Digit )+
			{
			// TSLexer.g:115:7: ( Digit )+
			int cnt7=0;
			loop7:
			while (true) {
				int alt7=2;
				int LA7_0 = input.LA(1);
				if ( ((LA7_0 >= '0' && LA7_0 <= '9')) ) {
					alt7=1;
				}

				switch (alt7) {
				case 1 :
					// TSLexer.g:
					{
					if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					if ( cnt7 >= 1 ) break loop7;
					EarlyExitException eee = new EarlyExitException(7, input);
					throw eee;
				}
				cnt7++;
			}

			mMINUS(); 

			// TSLexer.g:115:20: ( Digit )+
			int cnt8=0;
			loop8:
			while (true) {
				int alt8=2;
				int LA8_0 = input.LA(1);
				if ( ((LA8_0 >= '0' && LA8_0 <= '9')) ) {
					alt8=1;
				}

				switch (alt8) {
				case 1 :
					// TSLexer.g:
					{
					if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					if ( cnt8 >= 1 ) break loop8;
					EarlyExitException eee = new EarlyExitException(8, input);
					throw eee;
				}
				cnt8++;
			}

			mMINUS(); 

			// TSLexer.g:115:33: ( Digit )+
			int cnt9=0;
			loop9:
			while (true) {
				int alt9=2;
				int LA9_0 = input.LA(1);
				if ( ((LA9_0 >= '0' && LA9_0 <= '9')) ) {
					alt9=1;
				}

				switch (alt9) {
				case 1 :
					// TSLexer.g:
					{
					if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					if ( cnt9 >= 1 ) break loop9;
					EarlyExitException eee = new EarlyExitException(9, input);
					throw eee;
				}
				cnt9++;
			}

			match('T'); 
			// TSLexer.g:115:44: ( Digit )+
			int cnt10=0;
			loop10:
			while (true) {
				int alt10=2;
				int LA10_0 = input.LA(1);
				if ( ((LA10_0 >= '0' && LA10_0 <= '9')) ) {
					alt10=1;
				}

				switch (alt10) {
				case 1 :
					// TSLexer.g:
					{
					if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					if ( cnt10 >= 1 ) break loop10;
					EarlyExitException eee = new EarlyExitException(10, input);
					throw eee;
				}
				cnt10++;
			}

			mCOLON(); 

			// TSLexer.g:115:57: ( Digit )+
			int cnt11=0;
			loop11:
			while (true) {
				int alt11=2;
				int LA11_0 = input.LA(1);
				if ( ((LA11_0 >= '0' && LA11_0 <= '9')) ) {
					alt11=1;
				}

				switch (alt11) {
				case 1 :
					// TSLexer.g:
					{
					if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					if ( cnt11 >= 1 ) break loop11;
					EarlyExitException eee = new EarlyExitException(11, input);
					throw eee;
				}
				cnt11++;
			}

			mCOLON(); 

			// TSLexer.g:115:70: ( Digit )+
			int cnt12=0;
			loop12:
			while (true) {
				int alt12=2;
				int LA12_0 = input.LA(1);
				if ( ((LA12_0 >= '0' && LA12_0 <= '9')) ) {
					alt12=1;
				}

				switch (alt12) {
				case 1 :
					// TSLexer.g:
					{
					if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					if ( cnt12 >= 1 ) break loop12;
					EarlyExitException eee = new EarlyExitException(12, input);
					throw eee;
				}
				cnt12++;
			}

			// TSLexer.g:115:77: ( DOT ( Digit )+ )?
			int alt14=2;
			int LA14_0 = input.LA(1);
			if ( (LA14_0=='.') ) {
				alt14=1;
			}
			switch (alt14) {
				case 1 :
					// TSLexer.g:115:78: DOT ( Digit )+
					{
					mDOT(); 

					// TSLexer.g:115:82: ( Digit )+
					int cnt13=0;
					loop13:
					while (true) {
						int alt13=2;
						int LA13_0 = input.LA(1);
						if ( ((LA13_0 >= '0' && LA13_0 <= '9')) ) {
							alt13=1;
						}

						switch (alt13) {
						case 1 :
							// TSLexer.g:
							{
							if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
								input.consume();
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(null,input);
								recover(mse);
								throw mse;
							}
							}
							break;

						default :
							if ( cnt13 >= 1 ) break loop13;
							EarlyExitException eee = new EarlyExitException(13, input);
							throw eee;
						}
						cnt13++;
					}

					}
					break;

			}

			mPLUS(); 

			// TSLexer.g:115:96: ( Digit )+
			int cnt15=0;
			loop15:
			while (true) {
				int alt15=2;
				int LA15_0 = input.LA(1);
				if ( ((LA15_0 >= '0' && LA15_0 <= '9')) ) {
					alt15=1;
				}

				switch (alt15) {
				case 1 :
					// TSLexer.g:
					{
					if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					if ( cnt15 >= 1 ) break loop15;
					EarlyExitException eee = new EarlyExitException(15, input);
					throw eee;
				}
				cnt15++;
			}

			mCOLON(); 

			// TSLexer.g:115:109: ( Digit )+
			int cnt16=0;
			loop16:
			while (true) {
				int alt16=2;
				int LA16_0 = input.LA(1);
				if ( ((LA16_0 >= '0' && LA16_0 <= '9')) ) {
					alt16=1;
				}

				switch (alt16) {
				case 1 :
					// TSLexer.g:
					{
					if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					if ( cnt16 >= 1 ) break loop16;
					EarlyExitException eee = new EarlyExitException(16, input);
					throw eee;
				}
				cnt16++;
			}

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "DATETIME"

	// $ANTLR start "Integer"
	public final void mInteger() throws RecognitionException {
		try {
			int _type = Integer;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:119:2: ( ( '-' | '+' )? ( Digit )+ )
			// TSLexer.g:120:2: ( '-' | '+' )? ( Digit )+
			{
			// TSLexer.g:120:2: ( '-' | '+' )?
			int alt17=2;
			int LA17_0 = input.LA(1);
			if ( (LA17_0=='+'||LA17_0=='-') ) {
				alt17=1;
			}
			switch (alt17) {
				case 1 :
					// TSLexer.g:
					{
					if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

			}

			// TSLexer.g:120:15: ( Digit )+
			int cnt18=0;
			loop18:
			while (true) {
				int alt18=2;
				int LA18_0 = input.LA(1);
				if ( ((LA18_0 >= '0' && LA18_0 <= '9')) ) {
					alt18=1;
				}

				switch (alt18) {
				case 1 :
					// TSLexer.g:
					{
					if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					if ( cnt18 >= 1 ) break loop18;
					EarlyExitException eee = new EarlyExitException(18, input);
					throw eee;
				}
				cnt18++;
			}

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "Integer"

	// $ANTLR start "Float"
	public final void mFloat() throws RecognitionException {
		try {
			int _type = Float;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:125:2: ( ( '-' | '+' )? ( Digit )+ DOT ( Digit )+ ( ( 'e' | 'E' ) ( '-' | '+' )? ( Digit )+ )? )
			// TSLexer.g:126:2: ( '-' | '+' )? ( Digit )+ DOT ( Digit )+ ( ( 'e' | 'E' ) ( '-' | '+' )? ( Digit )+ )?
			{
			// TSLexer.g:126:2: ( '-' | '+' )?
			int alt19=2;
			int LA19_0 = input.LA(1);
			if ( (LA19_0=='+'||LA19_0=='-') ) {
				alt19=1;
			}
			switch (alt19) {
				case 1 :
					// TSLexer.g:
					{
					if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

			}

			// TSLexer.g:126:15: ( Digit )+
			int cnt20=0;
			loop20:
			while (true) {
				int alt20=2;
				int LA20_0 = input.LA(1);
				if ( ((LA20_0 >= '0' && LA20_0 <= '9')) ) {
					alt20=1;
				}

				switch (alt20) {
				case 1 :
					// TSLexer.g:
					{
					if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					if ( cnt20 >= 1 ) break loop20;
					EarlyExitException eee = new EarlyExitException(20, input);
					throw eee;
				}
				cnt20++;
			}

			mDOT(); 

			// TSLexer.g:126:26: ( Digit )+
			int cnt21=0;
			loop21:
			while (true) {
				int alt21=2;
				int LA21_0 = input.LA(1);
				if ( ((LA21_0 >= '0' && LA21_0 <= '9')) ) {
					alt21=1;
				}

				switch (alt21) {
				case 1 :
					// TSLexer.g:
					{
					if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					if ( cnt21 >= 1 ) break loop21;
					EarlyExitException eee = new EarlyExitException(21, input);
					throw eee;
				}
				cnt21++;
			}

			// TSLexer.g:126:33: ( ( 'e' | 'E' ) ( '-' | '+' )? ( Digit )+ )?
			int alt24=2;
			int LA24_0 = input.LA(1);
			if ( (LA24_0=='E'||LA24_0=='e') ) {
				alt24=1;
			}
			switch (alt24) {
				case 1 :
					// TSLexer.g:126:34: ( 'e' | 'E' ) ( '-' | '+' )? ( Digit )+
					{
					if ( input.LA(1)=='E'||input.LA(1)=='e' ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					// TSLexer.g:126:46: ( '-' | '+' )?
					int alt22=2;
					int LA22_0 = input.LA(1);
					if ( (LA22_0=='+'||LA22_0=='-') ) {
						alt22=1;
					}
					switch (alt22) {
						case 1 :
							// TSLexer.g:
							{
							if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
								input.consume();
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(null,input);
								recover(mse);
								throw mse;
							}
							}
							break;

					}

					// TSLexer.g:126:59: ( Digit )+
					int cnt23=0;
					loop23:
					while (true) {
						int alt23=2;
						int LA23_0 = input.LA(1);
						if ( ((LA23_0 >= '0' && LA23_0 <= '9')) ) {
							alt23=1;
						}

						switch (alt23) {
						case 1 :
							// TSLexer.g:
							{
							if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
								input.consume();
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(null,input);
								recover(mse);
								throw mse;
							}
							}
							break;

						default :
							if ( cnt23 >= 1 ) break loop23;
							EarlyExitException eee = new EarlyExitException(23, input);
							throw eee;
						}
						cnt23++;
					}

					}
					break;

			}

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "Float"

	// $ANTLR start "Identifier"
	public final void mIdentifier() throws RecognitionException {
		try {
			int _type = Identifier;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:130:5: ( ( Letter | Digit | '_' ) ( Letter | Digit | '_' )* )
			// TSLexer.g:131:5: ( Letter | Digit | '_' ) ( Letter | Digit | '_' )*
			{
			if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'Z')||input.LA(1)=='_'||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			// TSLexer.g:131:28: ( Letter | Digit | '_' )*
			loop25:
			while (true) {
				int alt25=2;
				int LA25_0 = input.LA(1);
				if ( ((LA25_0 >= '0' && LA25_0 <= '9')||(LA25_0 >= 'A' && LA25_0 <= 'Z')||LA25_0=='_'||(LA25_0 >= 'a' && LA25_0 <= 'z')) ) {
					alt25=1;
				}

				switch (alt25) {
				case 1 :
					// TSLexer.g:
					{
					if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'Z')||input.LA(1)=='_'||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					break loop25;
				}
			}

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "Identifier"

	// $ANTLR start "WS"
	public final void mWS() throws RecognitionException {
		try {
			int _type = WS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// TSLexer.g:135:5: ( ( ' ' | '\\r' | '\\t' | '\\n' ) )
			// TSLexer.g:135:8: ( ' ' | '\\r' | '\\t' | '\\n' )
			{
			if ( (input.LA(1) >= '\t' && input.LA(1) <= '\n')||input.LA(1)=='\r'||input.LA(1)==' ' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			 _channel=HIDDEN; 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "WS"

	@Override
	public void mTokens() throws RecognitionException {
		// TSLexer.g:1:8: ( KW_PRIVILEGES | KW_TIMESERIES | KW_ROLE | KW_GRANT | KW_REVOKE | KW_MERGE | KW_QUIT | KW_METADATA | KW_DATATYPE | KW_ENCODING | KW_STORAGE | KW_AND | KW_OR | KW_NOT | KW_ORDER | KW_GROUP | KW_BY | KW_WHERE | KW_FROM | KW_SELECT | KW_INSERT | KW_ON | KW_SHOW | KW_LOAD | KW_NULL | KW_CREATE | KW_DROP | KW_TO | KW_TIMESTAMP | KW_USER | KW_INTO | KW_WITH | KW_SET | KW_DELETE | KW_UPDATE | KW_VALUES | KW_VALUE | KW_PASSWORD | KW_DESCRIBE | KW_PROPERTY | KW_ADD | KW_LABEL | KW_LINK | KW_UNLINK | QUOTE | DOT | COLON | COMMA | SEMICOLON | LPAREN | RPAREN | EQUAL | EQUAL_NS | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN | DIVIDE | PLUS | MINUS | STAR | StringLiteral | DATETIME | Integer | Float | Identifier | WS )
		int alt26=68;
		alt26 = dfa26.predict(input);
		switch (alt26) {
			case 1 :
				// TSLexer.g:1:10: KW_PRIVILEGES
				{
				mKW_PRIVILEGES(); 

				}
				break;
			case 2 :
				// TSLexer.g:1:24: KW_TIMESERIES
				{
				mKW_TIMESERIES(); 

				}
				break;
			case 3 :
				// TSLexer.g:1:38: KW_ROLE
				{
				mKW_ROLE(); 

				}
				break;
			case 4 :
				// TSLexer.g:1:46: KW_GRANT
				{
				mKW_GRANT(); 

				}
				break;
			case 5 :
				// TSLexer.g:1:55: KW_REVOKE
				{
				mKW_REVOKE(); 

				}
				break;
			case 6 :
				// TSLexer.g:1:65: KW_MERGE
				{
				mKW_MERGE(); 

				}
				break;
			case 7 :
				// TSLexer.g:1:74: KW_QUIT
				{
				mKW_QUIT(); 

				}
				break;
			case 8 :
				// TSLexer.g:1:82: KW_METADATA
				{
				mKW_METADATA(); 

				}
				break;
			case 9 :
				// TSLexer.g:1:94: KW_DATATYPE
				{
				mKW_DATATYPE(); 

				}
				break;
			case 10 :
				// TSLexer.g:1:106: KW_ENCODING
				{
				mKW_ENCODING(); 

				}
				break;
			case 11 :
				// TSLexer.g:1:118: KW_STORAGE
				{
				mKW_STORAGE(); 

				}
				break;
			case 12 :
				// TSLexer.g:1:129: KW_AND
				{
				mKW_AND(); 

				}
				break;
			case 13 :
				// TSLexer.g:1:136: KW_OR
				{
				mKW_OR(); 

				}
				break;
			case 14 :
				// TSLexer.g:1:142: KW_NOT
				{
				mKW_NOT(); 

				}
				break;
			case 15 :
				// TSLexer.g:1:149: KW_ORDER
				{
				mKW_ORDER(); 

				}
				break;
			case 16 :
				// TSLexer.g:1:158: KW_GROUP
				{
				mKW_GROUP(); 

				}
				break;
			case 17 :
				// TSLexer.g:1:167: KW_BY
				{
				mKW_BY(); 

				}
				break;
			case 18 :
				// TSLexer.g:1:173: KW_WHERE
				{
				mKW_WHERE(); 

				}
				break;
			case 19 :
				// TSLexer.g:1:182: KW_FROM
				{
				mKW_FROM(); 

				}
				break;
			case 20 :
				// TSLexer.g:1:190: KW_SELECT
				{
				mKW_SELECT(); 

				}
				break;
			case 21 :
				// TSLexer.g:1:200: KW_INSERT
				{
				mKW_INSERT(); 

				}
				break;
			case 22 :
				// TSLexer.g:1:210: KW_ON
				{
				mKW_ON(); 

				}
				break;
			case 23 :
				// TSLexer.g:1:216: KW_SHOW
				{
				mKW_SHOW(); 

				}
				break;
			case 24 :
				// TSLexer.g:1:224: KW_LOAD
				{
				mKW_LOAD(); 

				}
				break;
			case 25 :
				// TSLexer.g:1:232: KW_NULL
				{
				mKW_NULL(); 

				}
				break;
			case 26 :
				// TSLexer.g:1:240: KW_CREATE
				{
				mKW_CREATE(); 

				}
				break;
			case 27 :
				// TSLexer.g:1:250: KW_DROP
				{
				mKW_DROP(); 

				}
				break;
			case 28 :
				// TSLexer.g:1:258: KW_TO
				{
				mKW_TO(); 

				}
				break;
			case 29 :
				// TSLexer.g:1:264: KW_TIMESTAMP
				{
				mKW_TIMESTAMP(); 

				}
				break;
			case 30 :
				// TSLexer.g:1:277: KW_USER
				{
				mKW_USER(); 

				}
				break;
			case 31 :
				// TSLexer.g:1:285: KW_INTO
				{
				mKW_INTO(); 

				}
				break;
			case 32 :
				// TSLexer.g:1:293: KW_WITH
				{
				mKW_WITH(); 

				}
				break;
			case 33 :
				// TSLexer.g:1:301: KW_SET
				{
				mKW_SET(); 

				}
				break;
			case 34 :
				// TSLexer.g:1:308: KW_DELETE
				{
				mKW_DELETE(); 

				}
				break;
			case 35 :
				// TSLexer.g:1:318: KW_UPDATE
				{
				mKW_UPDATE(); 

				}
				break;
			case 36 :
				// TSLexer.g:1:328: KW_VALUES
				{
				mKW_VALUES(); 

				}
				break;
			case 37 :
				// TSLexer.g:1:338: KW_VALUE
				{
				mKW_VALUE(); 

				}
				break;
			case 38 :
				// TSLexer.g:1:347: KW_PASSWORD
				{
				mKW_PASSWORD(); 

				}
				break;
			case 39 :
				// TSLexer.g:1:359: KW_DESCRIBE
				{
				mKW_DESCRIBE(); 

				}
				break;
			case 40 :
				// TSLexer.g:1:371: KW_PROPERTY
				{
				mKW_PROPERTY(); 

				}
				break;
			case 41 :
				// TSLexer.g:1:383: KW_ADD
				{
				mKW_ADD(); 

				}
				break;
			case 42 :
				// TSLexer.g:1:390: KW_LABEL
				{
				mKW_LABEL(); 

				}
				break;
			case 43 :
				// TSLexer.g:1:399: KW_LINK
				{
				mKW_LINK(); 

				}
				break;
			case 44 :
				// TSLexer.g:1:407: KW_UNLINK
				{
				mKW_UNLINK(); 

				}
				break;
			case 45 :
				// TSLexer.g:1:417: QUOTE
				{
				mQUOTE(); 

				}
				break;
			case 46 :
				// TSLexer.g:1:423: DOT
				{
				mDOT(); 

				}
				break;
			case 47 :
				// TSLexer.g:1:427: COLON
				{
				mCOLON(); 

				}
				break;
			case 48 :
				// TSLexer.g:1:433: COMMA
				{
				mCOMMA(); 

				}
				break;
			case 49 :
				// TSLexer.g:1:439: SEMICOLON
				{
				mSEMICOLON(); 

				}
				break;
			case 50 :
				// TSLexer.g:1:449: LPAREN
				{
				mLPAREN(); 

				}
				break;
			case 51 :
				// TSLexer.g:1:456: RPAREN
				{
				mRPAREN(); 

				}
				break;
			case 52 :
				// TSLexer.g:1:463: EQUAL
				{
				mEQUAL(); 

				}
				break;
			case 53 :
				// TSLexer.g:1:469: EQUAL_NS
				{
				mEQUAL_NS(); 

				}
				break;
			case 54 :
				// TSLexer.g:1:478: NOTEQUAL
				{
				mNOTEQUAL(); 

				}
				break;
			case 55 :
				// TSLexer.g:1:487: LESSTHANOREQUALTO
				{
				mLESSTHANOREQUALTO(); 

				}
				break;
			case 56 :
				// TSLexer.g:1:505: LESSTHAN
				{
				mLESSTHAN(); 

				}
				break;
			case 57 :
				// TSLexer.g:1:514: GREATERTHANOREQUALTO
				{
				mGREATERTHANOREQUALTO(); 

				}
				break;
			case 58 :
				// TSLexer.g:1:535: GREATERTHAN
				{
				mGREATERTHAN(); 

				}
				break;
			case 59 :
				// TSLexer.g:1:547: DIVIDE
				{
				mDIVIDE(); 

				}
				break;
			case 60 :
				// TSLexer.g:1:554: PLUS
				{
				mPLUS(); 

				}
				break;
			case 61 :
				// TSLexer.g:1:559: MINUS
				{
				mMINUS(); 

				}
				break;
			case 62 :
				// TSLexer.g:1:565: STAR
				{
				mSTAR(); 

				}
				break;
			case 63 :
				// TSLexer.g:1:570: StringLiteral
				{
				mStringLiteral(); 

				}
				break;
			case 64 :
				// TSLexer.g:1:584: DATETIME
				{
				mDATETIME(); 

				}
				break;
			case 65 :
				// TSLexer.g:1:593: Integer
				{
				mInteger(); 

				}
				break;
			case 66 :
				// TSLexer.g:1:601: Float
				{
				mFloat(); 

				}
				break;
			case 67 :
				// TSLexer.g:1:607: Identifier
				{
				mIdentifier(); 

				}
				break;
			case 68 :
				// TSLexer.g:1:618: WS
				{
				mWS(); 

				}
				break;

		}
	}


	protected DFA26 dfa26 = new DFA26(this);
	static final String DFA26_eotS =
		"\1\uffff\14\46\1\77\10\46\1\115\7\uffff\1\117\1\121\1\uffff\1\122\1\124"+
		"\2\uffff\1\127\2\uffff\3\46\1\135\16\46\1\161\1\162\2\46\2\uffff\1\165"+
		"\14\46\1\uffff\1\u0084\4\uffff\1\127\2\uffff\1\127\2\uffff\4\46\1\uffff"+
		"\16\46\1\u0097\1\46\1\u0099\1\u009a\1\46\2\uffff\1\77\1\46\1\uffff\15"+
		"\46\2\uffff\4\46\1\u00ae\5\46\1\u00b4\1\46\1\u00b6\5\46\1\uffff\1\u00bc"+
		"\2\uffff\1\46\1\u00be\1\46\1\u00c0\1\u00c1\1\46\1\u00c3\1\u00c4\1\46\1"+
		"\u00c6\1\46\1\u00c8\7\46\1\uffff\1\46\1\u00d2\1\u00d3\1\u00d4\1\46\1\uffff"+
		"\1\46\1\uffff\5\46\1\uffff\1\u00dc\1\uffff\1\u00dd\2\uffff\1\46\2\uffff"+
		"\1\u00df\1\uffff\1\46\1\uffff\2\46\1\u00e4\5\46\1\u00ea\3\uffff\2\46\1"+
		"\u00ed\3\46\1\u00f1\2\uffff\1\u00f2\1\uffff\1\u00f3\1\u00f4\1\u00f5\1"+
		"\u00f6\1\uffff\5\46\1\uffff\2\46\1\uffff\2\46\1\u0100\6\uffff\1\46\1\u0102"+
		"\1\u0103\2\46\1\u0106\1\u0107\1\u0108\1\u0109\1\uffff\1\46\2\uffff\1\46"+
		"\1\u010c\4\uffff\1\u010d\1\u010e\3\uffff";
	static final String DFA26_eofS =
		"\u010f\uffff";
	static final String DFA26_minS =
		"\1\11\1\101\1\111\1\105\1\122\1\105\1\125\1\101\1\116\1\105\1\104\1\116"+
		"\1\117\1\75\1\131\1\110\1\122\1\116\1\101\1\122\1\116\1\101\1\0\7\uffff"+
		"\2\75\1\uffff\2\60\2\uffff\1\55\2\uffff\1\111\1\123\1\115\1\60\1\114\1"+
		"\126\1\101\1\122\1\111\1\124\1\117\1\114\1\103\1\117\1\114\1\117\2\104"+
		"\2\60\1\124\1\114\2\uffff\1\60\1\105\1\124\1\117\1\123\1\101\1\102\1\116"+
		"\2\105\1\104\2\114\1\uffff\1\76\4\uffff\1\56\2\uffff\1\55\2\uffff\1\126"+
		"\1\120\1\123\1\105\1\uffff\1\105\1\117\1\116\1\125\1\107\1\101\1\124\1"+
		"\101\1\120\1\105\1\103\1\117\1\122\1\105\1\60\1\127\2\60\1\105\2\uffff"+
		"\1\60\1\114\1\uffff\1\122\1\110\1\115\1\105\1\117\1\104\1\105\1\113\1"+
		"\101\1\122\1\101\1\111\1\125\2\uffff\1\111\1\105\1\127\1\123\1\60\1\113"+
		"\1\124\1\120\1\105\1\104\1\60\1\124\1\60\1\124\1\122\1\104\1\101\1\103"+
		"\1\uffff\1\60\2\uffff\1\122\1\60\1\105\2\60\1\122\2\60\1\114\1\60\1\124"+
		"\1\60\1\124\1\116\1\105\1\114\1\122\1\117\1\105\1\uffff\1\105\3\60\1\101"+
		"\1\uffff\1\131\1\uffff\1\105\2\111\1\107\1\124\1\uffff\1\60\1\uffff\1"+
		"\60\2\uffff\1\124\2\uffff\1\60\1\uffff\1\105\1\uffff\1\105\1\113\1\60"+
		"\1\105\1\124\2\122\1\101\1\60\3\uffff\1\124\1\120\1\60\1\102\1\116\1\105"+
		"\1\60\2\uffff\1\60\1\uffff\4\60\1\uffff\1\107\1\131\1\104\1\111\1\115"+
		"\1\uffff\1\101\1\105\1\uffff\1\105\1\107\1\60\6\uffff\1\105\2\60\1\105"+
		"\1\120\4\60\1\uffff\1\123\2\uffff\1\123\1\60\4\uffff\2\60\3\uffff";
	static final String DFA26_maxS =
		"\1\172\1\122\2\117\1\122\1\105\1\125\1\122\1\116\1\124\1\116\1\122\1\125"+
		"\1\75\1\131\1\111\1\122\1\116\1\117\1\122\1\123\1\101\1\uffff\7\uffff"+
		"\1\76\1\75\1\uffff\2\71\2\uffff\1\172\2\uffff\1\117\1\123\1\115\1\172"+
		"\1\114\1\126\1\117\1\124\1\111\1\124\1\117\1\123\1\103\1\117\1\124\1\117"+
		"\2\104\2\172\1\124\1\114\2\uffff\1\172\1\105\1\124\1\117\1\124\1\101\1"+
		"\102\1\116\2\105\1\104\2\114\1\uffff\1\76\4\uffff\1\71\2\uffff\1\172\2"+
		"\uffff\1\126\1\120\1\123\1\105\1\uffff\1\105\1\117\1\116\1\125\1\107\1"+
		"\101\1\124\1\101\1\120\1\105\1\103\1\117\1\122\1\105\1\172\1\127\2\172"+
		"\1\105\2\uffff\1\172\1\114\1\uffff\1\122\1\110\1\115\1\105\1\117\1\104"+
		"\1\105\1\113\1\101\1\122\1\101\1\111\1\125\2\uffff\1\111\1\105\1\127\1"+
		"\123\1\172\1\113\1\124\1\120\1\105\1\104\1\172\1\124\1\172\1\124\1\122"+
		"\1\104\1\101\1\103\1\uffff\1\172\2\uffff\1\122\1\172\1\105\2\172\1\122"+
		"\2\172\1\114\1\172\1\124\1\172\1\124\1\116\1\105\1\114\1\122\1\117\1\124"+
		"\1\uffff\1\105\3\172\1\101\1\uffff\1\131\1\uffff\1\105\2\111\1\107\1\124"+
		"\1\uffff\1\172\1\uffff\1\172\2\uffff\1\124\2\uffff\1\172\1\uffff\1\105"+
		"\1\uffff\1\105\1\113\1\172\1\105\1\124\2\122\1\101\1\172\3\uffff\1\124"+
		"\1\120\1\172\1\102\1\116\1\105\1\172\2\uffff\1\172\1\uffff\4\172\1\uffff"+
		"\1\107\1\131\1\104\1\111\1\115\1\uffff\1\101\1\105\1\uffff\1\105\1\107"+
		"\1\172\6\uffff\1\105\2\172\1\105\1\120\4\172\1\uffff\1\123\2\uffff\1\123"+
		"\1\172\4\uffff\2\172\3\uffff";
	static final String DFA26_acceptS =
		"\27\uffff\1\56\1\57\1\60\1\61\1\62\1\63\1\64\2\uffff\1\73\2\uffff\1\76"+
		"\1\77\1\uffff\1\103\1\104\26\uffff\1\66\1\16\15\uffff\1\55\1\uffff\1\70"+
		"\1\71\1\72\1\74\1\uffff\1\75\1\100\1\uffff\1\101\1\102\4\uffff\1\34\23"+
		"\uffff\1\15\1\26\2\uffff\1\21\15\uffff\1\65\1\67\22\uffff\1\41\1\uffff"+
		"\1\14\1\51\23\uffff\1\3\5\uffff\1\7\1\uffff\1\33\5\uffff\1\27\1\uffff"+
		"\1\31\1\uffff\1\40\1\23\1\uffff\1\37\1\30\1\uffff\1\53\1\uffff\1\36\11"+
		"\uffff\1\4\1\20\1\6\7\uffff\1\17\1\22\1\uffff\1\52\4\uffff\1\45\5\uffff"+
		"\1\5\2\uffff\1\42\3\uffff\1\24\1\25\1\32\1\43\1\54\1\44\11\uffff\1\13"+
		"\1\uffff\1\50\1\46\2\uffff\1\10\1\11\1\47\1\12\2\uffff\1\35\1\1\1\2";
	static final String DFA26_specialS =
		"\26\uffff\1\0\u00f8\uffff}>";
	static final String[] DFA26_transitionS = {
			"\2\47\2\uffff\1\47\22\uffff\1\47\1\15\1\44\4\uffff\1\26\1\33\1\34\1\43"+
			"\1\41\1\31\1\42\1\27\1\40\12\45\1\30\1\32\1\36\1\35\1\37\2\uffff\1\12"+
			"\1\16\1\23\1\7\1\10\1\20\1\4\1\46\1\21\2\46\1\22\1\5\1\14\1\13\1\1\1"+
			"\6\1\3\1\11\1\2\1\24\1\25\1\17\3\46\4\uffff\1\46\1\uffff\32\46",
			"\1\51\20\uffff\1\50",
			"\1\52\5\uffff\1\53",
			"\1\55\11\uffff\1\54",
			"\1\56",
			"\1\57",
			"\1\60",
			"\1\61\3\uffff\1\63\14\uffff\1\62",
			"\1\64",
			"\1\66\2\uffff\1\67\13\uffff\1\65",
			"\1\71\11\uffff\1\70",
			"\1\73\3\uffff\1\72",
			"\1\74\5\uffff\1\75",
			"\1\76",
			"\1\100",
			"\1\101\1\102",
			"\1\103",
			"\1\104",
			"\1\106\7\uffff\1\107\5\uffff\1\105",
			"\1\110",
			"\1\113\1\uffff\1\112\2\uffff\1\111",
			"\1\114",
			"\0\44",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"\1\116\1\76",
			"\1\120",
			"",
			"\12\123",
			"\12\123",
			"",
			"",
			"\1\125\1\130\1\uffff\12\126\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"",
			"",
			"\1\131\5\uffff\1\132",
			"\1\133",
			"\1\134",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\1\136",
			"\1\137",
			"\1\140\15\uffff\1\141",
			"\1\142\1\uffff\1\143",
			"\1\144",
			"\1\145",
			"\1\146",
			"\1\147\6\uffff\1\150",
			"\1\151",
			"\1\152",
			"\1\153\7\uffff\1\154",
			"\1\155",
			"\1\156",
			"\1\157",
			"\12\46\7\uffff\3\46\1\160\26\46\4\uffff\1\46\1\uffff\32\46",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\1\163",
			"\1\164",
			"",
			"",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\1\166",
			"\1\167",
			"\1\170",
			"\1\171\1\172",
			"\1\173",
			"\1\174",
			"\1\175",
			"\1\176",
			"\1\177",
			"\1\u0080",
			"\1\u0081",
			"\1\u0082",
			"",
			"\1\u0083",
			"",
			"",
			"",
			"",
			"\1\130\1\uffff\12\123",
			"",
			"",
			"\1\125\1\130\1\uffff\12\126\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"",
			"",
			"\1\u0085",
			"\1\u0086",
			"\1\u0087",
			"\1\u0088",
			"",
			"\1\u0089",
			"\1\u008a",
			"\1\u008b",
			"\1\u008c",
			"\1\u008d",
			"\1\u008e",
			"\1\u008f",
			"\1\u0090",
			"\1\u0091",
			"\1\u0092",
			"\1\u0093",
			"\1\u0094",
			"\1\u0095",
			"\1\u0096",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\1\u0098",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\1\u009b",
			"",
			"",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\1\u009c",
			"",
			"\1\u009d",
			"\1\u009e",
			"\1\u009f",
			"\1\u00a0",
			"\1\u00a1",
			"\1\u00a2",
			"\1\u00a3",
			"\1\u00a4",
			"\1\u00a5",
			"\1\u00a6",
			"\1\u00a7",
			"\1\u00a8",
			"\1\u00a9",
			"",
			"",
			"\1\u00aa",
			"\1\u00ab",
			"\1\u00ac",
			"\1\u00ad",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\1\u00af",
			"\1\u00b0",
			"\1\u00b1",
			"\1\u00b2",
			"\1\u00b3",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\1\u00b5",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\1\u00b7",
			"\1\u00b8",
			"\1\u00b9",
			"\1\u00ba",
			"\1\u00bb",
			"",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"",
			"",
			"\1\u00bd",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\1\u00bf",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\1\u00c2",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\1\u00c5",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\1\u00c7",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\1\u00c9",
			"\1\u00ca",
			"\1\u00cb",
			"\1\u00cc",
			"\1\u00cd",
			"\1\u00ce",
			"\1\u00cf\16\uffff\1\u00d0",
			"",
			"\1\u00d1",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\1\u00d5",
			"",
			"\1\u00d6",
			"",
			"\1\u00d7",
			"\1\u00d8",
			"\1\u00d9",
			"\1\u00da",
			"\1\u00db",
			"",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"",
			"",
			"\1\u00de",
			"",
			"",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"",
			"\1\u00e0",
			"",
			"\1\u00e1",
			"\1\u00e2",
			"\12\46\7\uffff\22\46\1\u00e3\7\46\4\uffff\1\46\1\uffff\32\46",
			"\1\u00e5",
			"\1\u00e6",
			"\1\u00e7",
			"\1\u00e8",
			"\1\u00e9",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"",
			"",
			"",
			"\1\u00eb",
			"\1\u00ec",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\1\u00ee",
			"\1\u00ef",
			"\1\u00f0",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"",
			"",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"",
			"\1\u00f7",
			"\1\u00f8",
			"\1\u00f9",
			"\1\u00fa",
			"\1\u00fb",
			"",
			"\1\u00fc",
			"\1\u00fd",
			"",
			"\1\u00fe",
			"\1\u00ff",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"",
			"",
			"",
			"",
			"",
			"",
			"\1\u0101",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\1\u0104",
			"\1\u0105",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"",
			"\1\u010a",
			"",
			"",
			"\1\u010b",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"",
			"",
			"",
			"",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"\12\46\7\uffff\32\46\4\uffff\1\46\1\uffff\32\46",
			"",
			"",
			""
	};

	static final short[] DFA26_eot = DFA.unpackEncodedString(DFA26_eotS);
	static final short[] DFA26_eof = DFA.unpackEncodedString(DFA26_eofS);
	static final char[] DFA26_min = DFA.unpackEncodedStringToUnsignedChars(DFA26_minS);
	static final char[] DFA26_max = DFA.unpackEncodedStringToUnsignedChars(DFA26_maxS);
	static final short[] DFA26_accept = DFA.unpackEncodedString(DFA26_acceptS);
	static final short[] DFA26_special = DFA.unpackEncodedString(DFA26_specialS);
	static final short[][] DFA26_transition;

	static {
		int numStates = DFA26_transitionS.length;
		DFA26_transition = new short[numStates][];
		for (int i=0; i<numStates; i++) {
			DFA26_transition[i] = DFA.unpackEncodedString(DFA26_transitionS[i]);
		}
	}

	protected class DFA26 extends DFA {

		public DFA26(BaseRecognizer recognizer) {
			this.recognizer = recognizer;
			this.decisionNumber = 26;
			this.eot = DFA26_eot;
			this.eof = DFA26_eof;
			this.min = DFA26_min;
			this.max = DFA26_max;
			this.accept = DFA26_accept;
			this.special = DFA26_special;
			this.transition = DFA26_transition;
		}
		@Override
		public String getDescription() {
			return "1:1: Tokens : ( KW_PRIVILEGES | KW_TIMESERIES | KW_ROLE | KW_GRANT | KW_REVOKE | KW_MERGE | KW_QUIT | KW_METADATA | KW_DATATYPE | KW_ENCODING | KW_STORAGE | KW_AND | KW_OR | KW_NOT | KW_ORDER | KW_GROUP | KW_BY | KW_WHERE | KW_FROM | KW_SELECT | KW_INSERT | KW_ON | KW_SHOW | KW_LOAD | KW_NULL | KW_CREATE | KW_DROP | KW_TO | KW_TIMESTAMP | KW_USER | KW_INTO | KW_WITH | KW_SET | KW_DELETE | KW_UPDATE | KW_VALUES | KW_VALUE | KW_PASSWORD | KW_DESCRIBE | KW_PROPERTY | KW_ADD | KW_LABEL | KW_LINK | KW_UNLINK | QUOTE | DOT | COLON | COMMA | SEMICOLON | LPAREN | RPAREN | EQUAL | EQUAL_NS | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN | DIVIDE | PLUS | MINUS | STAR | StringLiteral | DATETIME | Integer | Float | Identifier | WS );";
		}
		@Override
		public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
			IntStream input = _input;
			int _s = s;
			switch ( s ) {
					case 0 : 
						int LA26_22 = input.LA(1);
						s = -1;
						if ( ((LA26_22 >= '\u0000' && LA26_22 <= '\uFFFF')) ) {s = 36;}
						else s = 77;
						if ( s>=0 ) return s;
						break;
			}
			NoViableAltException nvae =
				new NoViableAltException(getDescription(), 26, _s, input);
			error(nvae);
			throw nvae;
		}
	}

}
