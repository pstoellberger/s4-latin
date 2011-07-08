package io.s4.latin.persister;

import io.s4.latin.pojo.StreamRow;
import io.s4.persist.Persister;
import io.s4.processor.AbstractPE;
import io.s4.processor.EventAdvice;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;


public class LatinPersisterPE extends AbstractPE {

	private String id;

	private Persister persister;
	private int persistTime;
	private String persistKey = "persister:Latin";

	private boolean debug = false;
	private List<StreamRow> cache = new ArrayList<StreamRow>();


	public LatinPersisterPE() {};

	public LatinPersisterPE(String id, String statement) {
		this.id = id;
		processStatement();
	}

	@Override
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
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


	private void processStatement() {
		// TODO Auto-generated method stub

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
			persister.set(persistKey, outputCache, persistTime);
		} catch (Exception e) {
			e.printStackTrace();
			Logger.getLogger("s4").error(e);
		}
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
		str += " Statement [ "+ statement+ " ]";
		str += " Debug-output [ "+ debug + " ]";
		return str.toString();
	}

}
