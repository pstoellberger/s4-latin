package io.s4.examples.logstats.pe;

import io.s4.dispatcher.EventDispatcher;
import io.s4.latin.pojo.StreamRow;
import io.s4.latin.pojo.StreamRow.ValueType;
import io.s4.processor.AbstractPE;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AccessLogParserPE extends AbstractPE {

    private String id;
    private EventDispatcher dispatcher;
    private String outputStreamName;
    
    private String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
    private Pattern p = Pattern.compile(logEntryPattern);

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

    public void setId(String id) {
        this.id = id;
    }

    public void processEvent(StreamRow line) {
        if (line == null) {
            return;
        }
        Matcher matcher = p.matcher(((String)line.get("line")));
        if (!matcher.matches() || 
          9 != matcher.groupCount()) {
          System.err.println("Bad log entry (or problem with RE?):");
          System.err.println(line.get("line"));
          return;
        }

        StreamRow sr = new StreamRow();
        sr.set("resource", matcher.group(5).split(" ")[1], ValueType.STRING);
        sr.set("ip", matcher.group(1), ValueType.STRING);
        sr.set("date",  matcher.group(4), ValueType.STRING);
        sr.set("request", matcher.group(5), ValueType.STRING);
        sr.set("response", matcher.group(6), ValueType.STRING);
        sr.set("bytes", matcher.group(7), ValueType.STRING);
        if (!matcher.group(8).equals("-")) {
        	sr.set("referer", matcher.group(8), ValueType.STRING);
        }
        else {
        	sr.set("referer" , "", ValueType.STRING);
        }
        sr.set("browser", matcher.group(9), ValueType.STRING);
        sr.setKey("resource");
        
        dispatcher.dispatchEvent(outputStreamName, sr);
    }

	@Override
	public void output() {
		// TODO Auto-generated method stub

	}

    @Override
    public String getId() {
        return this.id;
    }


}
