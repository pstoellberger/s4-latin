package io.s4.examples.logstats.inputadapter;

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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

/**
 * @author pstoellberger
 *
 */
public class Tailer implements EventProducer, Runnable { 

	    private String logFile;
	    private long maxBackoffTime = 30 * 1000; // 5 seconds
	    private long messageCount = 0;
	    private long blankCount = 0;
	    private String streamName;

	    private LinkedBlockingQueue<String> messageQueue = new LinkedBlockingQueue<String>();
	    private Set<io.s4.listener.EventHandler> handlers = new HashSet<io.s4.listener.EventHandler>();

	    public void setlogFile(String urlString) {
	        this.logFile = urlString;
	    }

	    public void setMaxBackoffTime(long maxBackoffTime) {
	        this.maxBackoffTime = maxBackoffTime;
	    }

	    public void setStreamName(String streamName) {
	        this.streamName = streamName;
	    }

	    public void init() {
//	        for (int i = 0; i < 1; i++) {
	            Dequeuer dequeuer = new Dequeuer(1);
	            Thread t = new Thread(dequeuer);
	            t.start();
//	        }
	        (new Thread(this)).start();
	    }

	    public void run() {
	        long backoffTime = 1000;
	        while(!Thread.interrupted()) {
	            try {
	                openAndRead();
	            } catch (Exception e) {
	                Logger.getLogger("s4").error("Exception reading logfile", e);
	                try {
	                    Thread.sleep(backoffTime);
	                    System.out.println("Sleeping for :" + backoffTime/1000 + " seconds");
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

	    public void openAndRead() throws Exception {
	        URL url = new URL(logFile);
	        System.out.println("## Reading File: " + logFile);
	        URLConnection connection = url.openConnection();

	        InputStream is = connection.getInputStream();
	        InputStreamReader isr = new InputStreamReader(is);
	        BufferedReader br = new BufferedReader(isr);

	        String inputLine = null;
	        while ((inputLine = br.readLine()) != null) {
	            if (inputLine.trim().length() == 0) {
	                blankCount++;
	                continue;
	            }
	            messageCount++;
	            messageQueue.add(inputLine);
	        }
	        System.out.println(messageCount + " lines read ");
	        throw new Exception("nothing to read anymore");
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
	                    StreamRow sr = new StreamRow();
	                    sr.set("line", message, ValueType.STRING);
	                    EventWrapper ew = new EventWrapper(streamName, sr, null);
	                    for (io.s4.listener.EventHandler handler : handlers) {
	                        try {
	                            handler.processEvent(ew);
	                        } catch (Exception e) {
	                            Logger.getLogger("s4")
	                                  .error("Exception in raw event handler", e);
	                        }
	                    }
	                } catch (InterruptedException ie) {
	                    Thread.currentThread().interrupt();
	                } catch (Exception e) {
	                    Logger.getLogger("s4")
	                          .error("Exception processing message", e);
	                }
	            }
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

	}
