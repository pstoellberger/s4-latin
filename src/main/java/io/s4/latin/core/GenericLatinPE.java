package io.s4.latin.core;

import io.s4.dispatcher.EventDispatcher;
import io.s4.latin.fun.Select;
import io.s4.latin.fun.Where;
import io.s4.latin.parser.LatinParser;
import io.s4.latin.pojo.StreamRow;
import io.s4.processor.AbstractPE;
import io.s4.processor.EventAdvice;

import java.util.ArrayList;
import java.util.List;

public class GenericLatinPE extends AbstractLatinPE {

	private String id;
	private EventDispatcher dispatcher;
	private String outputStreamName;
	private String statement;
	private Select select;
	private String whereCondition;

	private boolean debug = false;

	public GenericLatinPE() {};

	public GenericLatinPE(String id, String outputStream, String statement) {
		this.id = id;
		this.outputStreamName = outputStream;
		this.statement = statement;
		processStatement();
	}

	public void processEvent(StreamRow row) {

		// TODO remove
		if (row == null) {
			return;
		}
		row = Where.process(row, whereCondition);

		if (row == null) {
			return;
		}

		row = select.process(row);
		if (isDebug()) {
			System.out.println(row);
		}
		
		

		dispatcher.dispatchEvent(outputStreamName, row);
	}


	@Override
	public void output() {
		// TODO Auto-generated method stub

	}


	private void processStatement() {
		if (statement != null) {
			outputStreamName = LatinParser.getStreamName(statement);
			select = new Select(LatinParser.getSelect(statement));
			whereCondition = LatinParser.getWhere(statement);
			List<String> keys = new ArrayList<String>();
			keys.add(LatinParser.getFrom(statement) + " *");
			setKeys(keys.toArray(new String[1]));
		}
	}

	@Override
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}


	public String getStatement() {
		return statement;
	}

	public void setStatement(String statement) {
		this.statement = statement;
		processStatement();
	}

	public EventDispatcher getDispatcher() {
		return dispatcher;
	}

	public void setDispatcher(EventDispatcher dispatcher) {
		this.dispatcher = dispatcher;
	}

	public String getOutputStreamName() {
		return outputStreamName;
	}

	public void setOutputStreamName(String outputStreamName) {
		this.outputStreamName = outputStreamName;
	}
	
	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
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
		str += " Select [ "+ select + " ]";
		str += " Where [ "+ whereCondition + " ]";
		str += " Debug-output [ "+ debug + " ]";
		return str.toString();
	}

}
