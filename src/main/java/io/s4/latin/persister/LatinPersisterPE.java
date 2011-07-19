package io.s4.latin.persister;

import io.s4.latin.core.ObjectUtil;
import io.s4.latin.parser.LatinParser;
import io.s4.latin.pojo.StreamRow;
import io.s4.persist.Persister;
import io.s4.processor.AbstractPE;
import io.s4.processor.EventAdvice;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;


public class LatinPersisterPE extends AbstractPE {

	private String id;
	private String statement;
	private Persister persister;
	private int cacheTime = 120;
	private String persistKey = "persister:Latin";

	private boolean debug = false;
	private List<StreamRow> cache = new ArrayList<StreamRow>();


	public LatinPersisterPE() {
	};

	public LatinPersisterPE(String id, String statement) {
		this.id = id;
		this.statement = statement;
		processStatement();
	}

	public void processStatement() {
		if (statement != null && statement.length() > 0) {
			String from = LatinParser.getPersistFrom(statement);
			
			if (from != null) {
				from = from.trim();
//				if (from.startsWith("debug")) {
//					this.debug = true;
//				}
			}
			else {
				throw new RuntimeException("You have to define an Input Stream Name for this Persister\n Source: " + this);
			}
			String className = LatinParser.getOutputClassName(statement);
			Object o = ObjectUtil.construct(className, LatinParser.getProperties(className, statement));

			if (o instanceof Persister) {
				setPersister((Persister) o );
			}
			else {
				throw new RuntimeException("Created object is not of type Persister - Class: " + className);
			}

			// TODO enable this as i know how to handle the .output() of AbstractPE and Persister methods
//			FrequencyType ft = LatinParser.getOutputType(statement);
//			if (ft != null) {
//				Integer freq = LatinParser.getOutputFrequency(statement);
//				switch (ft) {
//				case EVENTCOUNT:
//					setOutputFrequencyByEventCount(freq);
//					break;
//				case TIMEBOUNDARY:
//					setOutputFrequencyByTimeBoundary(freq);
//					break;
//				}
//			}

			List<String> keys = new ArrayList<String>();
			keys.add(from+ " *");
			setKeys(keys.toArray(new String[1]));
		}
	}
	public void processEvent(StreamRow row) {

		// TODO remove
		if (row == null) {
			return;
		}
		if (isDebug()) {
			System.out.println(row);
		}
		cache.add(row);
	}


	@Override
	public void output() {
		List<StreamRow> outputCache = cache;
		cache = new ArrayList<StreamRow>();
		try {
			persister.set(persistKey, outputCache, cacheTime);
		} catch (Exception e) {
			e.printStackTrace();
			Logger.getLogger("s4").error(e);
		}
	}



	@Override
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
	public void setStatement(String statement) {
		this.statement = statement;
		processStatement();
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	public Persister getPersister() {
		return persister;
	}

	public void setPersister(Persister persister) {
		this.persister = persister;
	}

	@Override
	public String toString() {
		String str = "Bean: " + super.toString() + " : ";
		str += " ID [ "+ getId()+ " ], ";
		str += " persister [ "+ persister + " ], ";
		String advise =" advise [ ";
		for (EventAdvice adv : advise()) {
			advise += "( event=" + adv.getEventName() + " , key=" + adv.getKey() + ")";
		}
		advise += " ] ,";
		str +=  advise ;
		str += " Debug-output [ "+ debug + " ]";
		return str.toString();
	}

}
