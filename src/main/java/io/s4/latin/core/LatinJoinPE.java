package io.s4.latin.core;

import io.s4.dispatcher.EventDispatcher;
import io.s4.latin.parser.LatinParser;
import io.s4.latin.pojo.StreamRow;
import io.s4.processor.EventAdvice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

public class LatinJoinPE extends GenericLatinPE {

	private static Logger logger = Logger.getLogger(LatinJoinPE.class);
	private HashMap<String, ArrayList<StreamRow>> eventsToJoin;
	private Map<String, List<String>> eventFields = new HashMap<String, List<String>>();
	private Map<String, String> eventKeys = new HashMap<String, String>();
	private String outputStreamName;
	private EventDispatcher dispatcher;
	private boolean debug = false;
	private String statement;


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
		//		if (eventKeys.get(stream) == null) {
		//			eventKeys.put(stream,new ArrayList<String>());
		//		}
		//		eventKeys.get(stream).add(key);
		eventKeys.put(stream, key);
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

			String within = LatinParser.getWithin(statement);

			if (within != null) {
				String[] tokens = within.trim().split(" ");
				if (tokens.length == 2) {
					String strttl = tokens[0];
					int ttl = Integer.parseInt(strttl);
					System.err.println("WITHIN:" + ttl);
					setTtl(ttl);
				}
				else {
					System.err.println("ERROR: within contains invalid number of tokens WITHIN xxx SECONDS, but was \""+ within + "\"");
				}
			}


		}
	}

	public void processEvent(StreamRow event) {
		
		if (eventsToJoin == null) {
			eventsToJoin = new HashMap<String, ArrayList<StreamRow>>();
			eventsToJoin.put(getStreamName(),new ArrayList<StreamRow>());
		}
		System.out.println("STREAMROW EVENT: " + getStreamName() + " : size= " + (getStreamName() == null || eventsToJoin.get(getStreamName()) == null ? "NULL ": eventsToJoin.get(getStreamName()).size()));
		
		List<String> fieldNames = eventFields.get(getStreamName());
		if (fieldNames == null) {
			return;
		}

		// we only use the last event that comes through on the given stream

		if (getStreamName() != null && eventsToJoin.get(getStreamName()) == null) {
			eventsToJoin.put(getStreamName(),new ArrayList<StreamRow>());
		}
		
		eventsToJoin.get(getStreamName()).add(event);
		
		if (debug) {
//			System.out.println("Adding from Stream: " + getStreamName() + " Event: " + event);
		}
		String eKey = eventKeys.get(getStreamName());
		Object keyObj = event.get(eKey);
		if (eventsToJoin.keySet().size() == eventFields.keySet().size()) {

			for (String streamName : eventsToJoin.keySet()) {
				if (streamName.equals(getStreamName()) || eventsToJoin.get(getStreamName()).size() < 1) {
					continue;
				}
				for (StreamRow partialEvent : eventsToJoin.get(streamName)) {


					String otherKey = eventKeys.get(streamName);
					Object other = partialEvent.get(otherKey);

//					System.out.println("Comparing from Stream: " + streamName + " and " + getStreamName() + " keys " + otherKey + " and "+ eKey + "objects: " + keyObj + " vs  " + other);
					if (keyObj != null && other != null && (keyObj.equals(other) || keyObj == other)) {
						System.out.println("YES:" + other + " = " + keyObj);
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
						
						includeFields = eventFields.get(getStreamName());
						if (includeFields.size() == 1
								&& includeFields.get(0).equals("*")) {
							for (String key : event.getKeys()) {
								newEvent.set(streamName + "-" + key, event.get(key), event.getValueMeta(key));
							}
						} 
						else {
							for (String key : includeFields) {
								newEvent.set(streamName + "-" + key, event.get(key), event.getValueMeta(key));
							}
						}
						
						dispatcher.dispatchEvent(outputStreamName, newEvent);
						if (logger.isDebugEnabled()) {
							logger.debug("STEP 7 (JoinPE): " + newEvent.toString());
						}

						

					}
				}
			}

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
