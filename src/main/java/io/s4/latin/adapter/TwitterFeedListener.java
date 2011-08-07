/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 	        http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package io.s4.latin.adapter;

import io.s4.collector.EventWrapper;
import io.s4.latin.pojo.StreamRow;
import io.s4.latin.pojo.StreamRow.ValueType;
import io.s4.listener.EventHandler;
import io.s4.listener.EventProducer;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.util.EncodingUtil;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.JsonArray;

public class TwitterFeedListener implements ISource, EventProducer, Runnable {
	private String userid;
	private String password;
	private String urlString;
	private long maxBackoffTime = 30 * 1000; // 5 seconds
	private long messageCount = 0;
	private long blankCount = 0;
	private String streamName;

	private LinkedBlockingQueue<String> messageQueue = new LinkedBlockingQueue<String>();
	private Set<io.s4.listener.EventHandler> handlers = new HashSet<io.s4.listener.EventHandler>();

	public TwitterFeedListener(Properties props) {
		if (props != null) {
			if (props.getProperty("user") != null) {
				userid = props.getProperty("user");
			}
			if (props.getProperty("password") != null) {
				password = props.getProperty("password");
			}
			if (props.getProperty("url") != null) {
				urlString = props.getProperty("url");
			}
			if (props.getProperty("maxBackoffTime") != null) {
				maxBackoffTime = Long.parseLong(props.getProperty("maxBackoffTime"));
			}
			if (props.getProperty("stream") != null) {
				streamName = props.getProperty("stream");
			}
		}
	}
	public void setUserid(String userid) {
		this.userid = userid;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setUrlString(String urlString) {
		this.urlString = urlString;
	}

	public void setMaxBackoffTime(long maxBackoffTime) {
		this.maxBackoffTime = maxBackoffTime;
	}

	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}

	public void init() {
		for (int i = 0; i < 12; i++) {
			Dequeuer dequeuer = new Dequeuer(i);
			Thread t = new Thread(dequeuer);
			t.start();
		}
		(new Thread(this)).start();
	}

	public void run() {
		long backoffTime = 1000;
		while (!Thread.interrupted()) {

			try {
				connectAndRead();
			} catch (Exception e) {
				e.printStackTrace();
				Logger.getLogger("s4").error("Exception reading feed", e);
				try {
					Thread.sleep(backoffTime);
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
				}
				backoffTime = backoffTime * 2;
				if (backoffTime > maxBackoffTime) {
					backoffTime = maxBackoffTime;
				}
			}
		}
	}

	public void connectAndRead() throws Exception {
		URL url = new URL(urlString);
		URLConnection connection = url.openConnection();
		connection.setConnectTimeout(10000);
		connection.setReadTimeout(10000);

		String userPassword = userid + ":" + password;
		System.out.println("connect to " + connection.getURL().toString() + " ...");

		String encoded = EncodingUtil.getAsciiString(Base64.encodeBase64(EncodingUtil.getAsciiBytes(userPassword)));
		connection.setRequestProperty("Authorization", "Basic " + encoded);
		connection.setRequestProperty("Accept-Charset", "utf-8,ISO-8859-1");
		connection.connect();
		System.out.println("Connect OK!");
		System.out.println("Reading TwitterFeed ....");
		Long startTime = new Date().getTime();
		InputStream is = connection.getInputStream();
	    Charset utf8 = Charset.forName("UTF-8");

		InputStreamReader isr = new InputStreamReader(is, utf8);
		BufferedReader br = new BufferedReader(isr);

		String inputLine = null;

		while ((inputLine = br.readLine()) != null) {
			if (inputLine.trim().length() == 0) {
				blankCount++;
				continue;
			}
			messageCount++;
			messageQueue.add(inputLine);
			if (messageCount % 500 == 0) {
				Long currentTime = new Date().getTime();
				System.out.println("Lines processed: " + messageCount + "\t ( " + blankCount + " empty lines ) in " + (currentTime-startTime)/1000 + " seconds. Reading "+ messageCount / ((currentTime-startTime)/1000) + " rows/second");
			}
		}
	}

	class Dequeuer implements Runnable {
		private int id;

		public Dequeuer(int id) {
			this.id = id;
		}

		public void run() {
			while (!Thread.interrupted()) {
				try {
					String message = messageQueue.take();
					JSONObject jsonObject = new JSONObject(message);

					// ignore delete records for now
					if (jsonObject.has("delete")) {
						continue;
					}

					StreamRow row = getStatus(jsonObject);
					EventWrapper ew = new EventWrapper(streamName, row, null);
					for (io.s4.listener.EventHandler handler : handlers) {
						try {
							handler.processEvent(ew);
						} catch (Exception e) {
							e.printStackTrace();
							Logger.getLogger("s4")
							.error("Exception in raw event handler", e);
						}
					}
				} catch (InterruptedException ie) {
					ie.printStackTrace();
					Thread.currentThread().interrupt();
				} catch (Exception e) {
					e.printStackTrace();
					Logger.getLogger("s4")
					.error("Exception processing message", e);
				}
			}
		}

		public StreamRow getStatus(JSONObject jsonObject) {
			try {
				if (jsonObject == null || jsonObject.equals(JSONObject.NULL)) {
					return null;
				}

				StreamRow status = new StreamRow();


				Object value = jsonObject.opt("id");
				if (value != null && !value.equals(JSONObject.NULL)) {
					status.set("id", value, ValueType.STRING);
				}

				value = jsonObject.opt("in_reply_to_status_id");
				if (value != null && !value.equals(JSONObject.NULL)) {
					status.set("in_reply_to_status_id", ((Number) value), ValueType.NUMBER);

				}

				value = jsonObject.opt("text");
				if (value != null && !value.equals(JSONObject.NULL)) {
					status.set("text", ((String) value), ValueType.STRING);
				}

				value = jsonObject.opt("truncated");
				if (value != null && !value.equals(JSONObject.NULL)) {
					status.set("truncated", ((Boolean) value).toString(), ValueType.STRING);

				}

				value = jsonObject.opt("source");
				if (value != null && !value.equals(JSONObject.NULL)) {
					status.set("source", ((String) value), ValueType.STRING);
				}

				value = jsonObject.opt("in_reply_to_screen_name");
				if (value != null && !value.equals(JSONObject.NULL)) {
					status.set("in_reply_to_screen_name", ((String) value), ValueType.STRING);
				}
				
				value = jsonObject.opt("retweet_count");
				if (value != null && !value.equals(JSONObject.NULL)) {
					status.set("retweet_count", value, ValueType.STRING);

				}


				value = jsonObject.opt("favorited");
				if (value != null && !value.equals(JSONObject.NULL)) {
					status.set("favorited", ((Boolean) value).toString(), ValueType.STRING);
				}

				value = jsonObject.opt("in_reply_to_user_id");
				if (value != null && !value.equals(JSONObject.NULL)) {
					status.set("in_reply_to_user_id", ((Number) value), ValueType.NUMBER);
				}

				value = jsonObject.opt("created_at");
				if (value != null && !value.equals(JSONObject.NULL)) {
					status.set("created_at", ((String) value), ValueType.STRING);
				}

				JSONObject ent = jsonObject.getJSONObject("entities");
				JSONArray hashtags = null;

				if (ent != null) {
					hashtags = ent.optJSONArray("hashtags");
				}
				for(int i=0;i<5;i++) {
					String name = "hashtag" +i;
					Object val = "";
					if (hashtags != null && hashtags.length() >= i+1) {
						val = hashtags.getJSONObject(i).get("text");
					}
					status.set(name, val, ValueType.STRING);

				}

				return status;
			} catch (Exception e) {
				e.printStackTrace();
				Logger.getLogger("s4").error(e.fillInStackTrace());
			}

			return null;
		}


	}

	@Override
	public void addHandler(EventHandler handler) {
		handlers.add(handler);

	}

	@Override
	public boolean removeHandler(EventHandler handler) {
		return handlers.remove(handler);
	}

	@Override
	public String toString() {
		String str = super.toString();
		str += " [ user = " + userid + " ]";
		str += " [ password = " + password + " ]";
		str += " [ url = " + urlString + " ]";
		str += " [ maxBackoffTime = " + maxBackoffTime + " ]";
		str += " [ stream = " + streamName + " ]";
		return str;
	}

	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty("user", "");
		props.setProperty("password", "");
		props.setProperty("url", "http://stream.twitter.com/1/statuses/sample.json");
		props.setProperty("stream", "TestStream");

		TwitterFeedListener a = new TwitterFeedListener(props);
		a.init();
	}

}
