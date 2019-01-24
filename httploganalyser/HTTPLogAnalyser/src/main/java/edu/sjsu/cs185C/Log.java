package edu.sjsu.cs185C;

import java.io.Serializable;
import java.lang.String;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Log implements Serializable {
	private static final long serialVersionUID = 1L;

	private String ipAddress;
	private String clientIdentd;
	private String userID;
	private String dateTimeString;
	private String method;
	private String resource;
	private String protocol;
	private int responseCode;
	private long contentSize;
	private boolean isValid;

	private Log() {
		this.isValid = false;
	}

	private Log(String ipAddress, String clientIdentd, String userID, String dateTime, String method, String resource,
			String protocol, String responseCode, String contentSize) {
		this.ipAddress = ipAddress;
		this.clientIdentd = clientIdentd;
		this.userID = userID;
		this.dateTimeString = dateTime;
		this.method = method;
		this.resource = resource;
		this.protocol = protocol;
		this.responseCode = Integer.parseInt(responseCode);
		this.contentSize = Long.parseLong(contentSize);
		this.isValid = true;
	}

	// Example Apache log line:
	// 127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048
	private static final String LOG_REGEX =
			// 1:IP 2:client 3:user 4:date time 5:method 6:req 7:proto 8:respcode 9:size
			"^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
	private static Pattern LOG_PATTERN = Pattern.compile(LOG_REGEX);

	public static Function<ConsumerRecord<Integer, String>, Log> parseRawLog = (r) -> {
		Matcher m = LOG_PATTERN.matcher(r.value());
		if (!m.find()) {
			return new Log();
		}
		return new Log(m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8),
				m.group(9));
	};

	public static Function2<Integer, Integer, Integer> reduceBySum = (v1, v2) -> {
		return v1 + v2;
	};

	public static PairFunction<Log, Integer, Integer> mapResponseCode = (l) -> {
		return new Tuple2<Integer, Integer>(l.getResponseCode(), 1);
	};

	public static PairFunction<Log, String, Integer> mapIpAddress = (l) -> {
		return new Tuple2<String, Integer>(l.getIpAddress(), 1);
	};

	public static PairFunction<Log, String, Integer> mapResource = (l) -> {
		return new Tuple2<String, Integer>(l.getResource(), 1);
	};

	public static PairFunction<Log, String, Integer> mapResourceError = (l) -> {
		if (l.getResponseCode() >= 400)
			return new Tuple2<String, Integer>(l.getResource(), 1);
		return new Tuple2<String, Integer>(l.getResource(), 0);
	};
	
	public static Function<Tuple2<Integer, Integer>, Boolean> responseCode4xx = (l) -> {
		if ((l._1()/100) == 4)
			return true;
		return false;
	};
	
	public static Function<Tuple2<Integer, Integer>, Boolean> responseCode5xx = (l) -> {
		if ((l._1()/100) == 5)
			return true;
		return false;
	};

	public static Function<Tuple2<String, Integer>, Boolean> checkValidTuple = (l) -> {
		if (l._2() != 0)
			return true;
		return false;
	};

	public static Function<Log, Boolean> checkValidLog = (l) -> {
		return l.isValid();
	};

	@Override
	public String toString() {
		return String.format("%s %s %s [%s] \"%s %s %s\" %s %s", ipAddress, clientIdentd, userID, dateTimeString,
				method, resource, protocol, responseCode, contentSize);
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public String getClientIdentd() {
		return clientIdentd;
	}

	public String getUserID() {
		return userID;
	}

	public String getDateTimeString() {
		return dateTimeString;
	}

	public String getMethod() {
		return method;
	}

	public String getResource() {
		return resource;
	}

	public String getProtocol() {
		return protocol;
	}

	public long getContentSize() {
		return contentSize;
	}

	public int getResponseCode() {
		return responseCode;
	}

	public boolean isValid() {
		return isValid;
	}

	public void setValid(boolean isValid) {
		this.isValid = isValid;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public void setClientIdentd(String clientIdentd) {
		this.clientIdentd = clientIdentd;
	}

	public void setUserID(String userID) {
		this.userID = userID;
	}

	public void setDateTimeString(String dateTimeString) {
		this.dateTimeString = dateTimeString;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public void setResource(String resource) {
		this.resource = resource;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}

	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
	}

	public void setContentSize(long contentSize) {
		this.contentSize = contentSize;
	}
}
