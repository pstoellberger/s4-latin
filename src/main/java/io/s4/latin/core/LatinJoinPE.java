package io.s4.latin.core;

import io.s4.dispatcher.EventDispatcher;
import io.s4.latin.parser.LatinParser;
import io.s4.latin.pojo.StreamRow;
import io.s4.persist.ConMapPersister;
import io.s4.persist.HashMapPersister;
import io.s4.processor.EventAdvice;
import io.s4.util.clock.Clock;
import io.s4.util.clock.WallClock;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

public class LatinJoinPE extends GenericLatinPE {

	private static Logger logger = Logger.getLogger(LatinJoinPE.class);
	private HashMap<String, HashMapPersister> eventsToJoin;
	private Map<String, List<String>> eventFields = new HashMap<String, List<String>>();
	private Map<String, String> eventKeys = new HashMap<String, String>();
	private String outputStreamName;
	private EventDispatcher dispatcher;
	private boolean debug = false;
	private String statement;
	
	private Long startTime;

	private int windowSize = 3600; // default window size is 1h
	private int windowInterval = 60;

	Integer keyCount = 0;

	public void setDispatcher(EventDispatcher dispatcher) {
		this.dispatcher = dispatcher;
	}

	public EventDispatcher getDispatcher() {
		return dispatcher;
	}

	public void setOutputStreamName(String outputStreamName) {
		this.outputStreamName = outputStreamName;
	}

	public String getOutputStreamName() {
		return outputStreamName;
	}

	public String getStatement() {
		return statement;
	}

	public void setStatement(String statement) {
		try {
			this.statement = statement;
			processStatement();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void addRealKey(String stream, String key) {
		eventKeys.put(stream, key);
	}

	@Override
	public void output() {
		super.output();
		
        System.out.println("Output timestamp: " + (new Date()).toString());
        System.out.println("Output time since start:" + ((new Date()).getTime() - startTime) / 1000 + " seconds");

        
		int outputCount  = 0;
		String outer = null;
		int outerSize = -1;
		for (String streams : eventsToJoin.keySet()) {
			if (outerSize < 0 || outerSize < eventsToJoin.get(streams).getCacheEntryCount()) {
				outer = streams;
			}
		}
		for (String lkey : eventsToJoin.get(outer).keySet()) {

			StreamRow event = (StreamRow) eventsToJoin.get(outer).get(lkey);
			String eKey = eventKeys.get(outer);
			Object keyObj = event.get(eKey);
			if (eventsToJoin.keySet().size() == eventFields.keySet().size()) {
				for (String streamName : eventsToJoin.keySet()) {
					if (streamName.equals(outer) || eventsToJoin.get(outer).getPersistCount() < 1) {
						continue;
					}
					for (String k : eventsToJoin.get(streamName).keySet()) {

						StreamRow partialEvent = (StreamRow) eventsToJoin.get(streamName).get(k);

						String otherKey = eventKeys.get(streamName);
						Object other = partialEvent.get(otherKey);

//						System.out.println("Comparing from Stream: " + streamName + " and " + outer + " keys " + otherKey + " and "+ eKey + "objects: " + keyObj + " vs  " + other);
						if (keyObj != null && other != null && (keyObj.equals(other) || keyObj == other)) {
//							System.out.println("YES:" + other + " = " + keyObj);
							StreamRow  newEvent = new StreamRow();

							List<String> includeFields = eventFields.get(streamName);
							if (includeFields.size() == 1
									&& includeFields.get(0).equals("*")) {
								for (String key : partialEvent.getKeys()) {
									newEvent.set(streamName + "-" + key, partialEvent.get(key), partialEvent.getValueMeta(key));
								}
							} 
							else {
								for (String key : includeFields) {
									newEvent.set(streamName + "-" + key, partialEvent.get(key), partialEvent.getValueMeta(key));
								}
							}

							includeFields = eventFields.get(outer);
							if (includeFields.size() == 1
									&& includeFields.get(0).equals("*")) {
								for (String key : event.getKeys()) {
									newEvent.set(outer + "-" + key, event.get(key), event.getValueMeta(key));
								}
							} 
							else {
								for (String key : includeFields) {
									newEvent.set(outer + "-" + key, event.get(key), event.getValueMeta(key));
								}
							}

							outputCount++;
							dispatcher.dispatchEvent(outputStreamName, newEvent);
							if (logger.isDebugEnabled()) {
								logger.debug("STEP 7 (JoinPE): " + newEvent.toString());
							}



						}
					}
				}
			}
		}
		for (String key : eventsToJoin.keySet()) {
			int cleanCount = eventsToJoin.get(key).cleanOutGarbage();
			System.out.println("Stream:" + key + " Window cache size: "
            + eventsToJoin.get(key).getCacheEntryCount() + " entries");
		}
		System.out.println("Windowed output count:" + outputCount);

	}
	private void processStatement() {
		if (statement != null) {
			outputStreamName = LatinParser.getStreamName(statement);

			String[] keys = LatinParser.getJoinKeys(statement);
			if (keys != null) {
				for (int i=0; i < keys.length; i++) {

					String[] key= keys[i].split(" ");
					String k = "" + key[0].trim() + "";
					String v = "" + key[1].trim() + "";
					addRealKey(k,v);

					System.err.println("RealKeys: " + k + " : " + v);

					keys[i] = key[0] + " *";
					System.err.println("Keys: " + keys[i]);

					String within = LatinParser.getWindow(statement);

					if (within != null) {
						String[] tokens = within.trim().split(" ");
						if (tokens.length >= 2) {
							String window = tokens[0];
							windowSize  = Integer.parseInt(window);
							String interval = tokens[3];
							windowInterval = Integer.parseInt(interval);
						}
					}
					System.err.println("Window Size: " + windowSize + " Interval: " + windowInterval);
				}

				setKeys(keys);
			}
			else {
				throw new RuntimeException("No keys are set for join. Keys:" + keys);
			}

			String[] includes = LatinParser.getJoinIncludes(statement);
			for (String include : includes) {
				System.err.println("Include: " + include);
			}
			setIncludeFields(includes);

		}
	}

	public void processEvent(StreamRow event) {

		if (eventsToJoin == null) {
			startTime = (new Date()).getTime();
			setOutputFrequencyByTimeBoundary(windowInterval);
			Clock a = new WallClock();
			eventsToJoin = new HashMap<String, HashMapPersister>();
			HashMapPersister c =  new HashMapPersister(a);
			c.init();
			eventsToJoin.put(getStreamName(),c);
		}
//		System.out.println("STREAMROW EVENT: " + getStreamName() + " : size= " + (getStreamName() == null || eventsToJoin.get(getStreamName()) == null ? "NULL ": eventsToJoin.get(getStreamName()).getCacheEntryCount()));

		List<String> fieldNames = eventFields.get(getStreamName());
		if (fieldNames == null) {
			return;
		}

		if (getStreamName() != null && eventsToJoin.get(getStreamName()) == null) {
			Clock a = new WallClock();
			HashMapPersister c =  new HashMapPersister(a);
			c.init();
			eventsToJoin.put(getStreamName(),c);
		}
		//		System.out.println("Adding from Stream: " + getStreamName() + " Event: " + event);

		HashMapPersister m = eventsToJoin.get(getStreamName());
		m.set("" + (event.toString().hashCode() + keyCount), event, windowSize);
		keyCount++;
		if (debug) {
			//			System.out.println("Adding from Stream: " + getStreamName() + " Event: " + event);
		}




	}


	public boolean isDebug() {
		return debug ;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	public void setIncludeFields(String[] includeFields) {
		for (String includeField : includeFields) {
			StringTokenizer st = new StringTokenizer(includeField);
			if (st.countTokens() != 2) {
				Logger.getLogger("s4").error("Bad include field specified: "
						+ includeField);
				continue;
			}

			String eventName = st.nextToken();
			String fieldName = st.nextToken();

			List<String> fieldNames = eventFields.get(eventName);
			if (fieldNames == null) {
				fieldNames = new ArrayList<String>();
				eventFields.put(eventName, fieldNames);
			}

			if (fieldName.equals("*")) {
				fieldNames.clear();
				fieldNames.add("*");
			} else {
				fieldNames.add(fieldName);
			}
		}
	}
	@Override
	public String toString() {
		String str = "Bean: " + super.toString() + " : ";
		str += " ID [ "+ getId()+ " ], ";
		str += " dispatcher [ "+ getDispatcher().toString() + " ], ";
		str += " outputstream [ "+ getOutputStreamName() + " ], ";
		String advise =" advise [ ";
		for (EventAdvice adv : advise()) {
			advise += "( event=" + adv.getEventName() + " , key=" + adv.getKey() + ")";
		}
		advise += " ] ,";
		str +=  advise ;
		str += " Statement [ "+ statement+ " ]";
		str += " Debug-output [ "+ debug + " ]";
		return str.toString();
	}


}
