package io.s4.latin.adapter;

import io.s4.collector.EventWrapper;
import io.s4.dispatcher.partitioner.CompoundKeyInfo;
import io.s4.latin.pojo.PojoUtil;
import io.s4.latin.pojo.StreamRow;
import io.s4.latin.pojo.StreamRow.ValueType;
import io.s4.listener.EventHandler;
import io.s4.listener.EventProducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.vfs.FileContent;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileSystemManager;
import org.apache.commons.vfs.RandomAccessContent;
import org.apache.commons.vfs.VFS;
import org.apache.commons.vfs.util.RandomAccessMode;
import org.apache.log4j.Logger;

/**
 * @author pstoellberger
 *
 */
public class VfsFileReader implements ISource, EventProducer, Runnable { 

	private static long INITIAL_WAIT_TIME = 1000;
	private String file;
	private long maxBackoffTime = 10 * 1000;
	private long backOffTime = INITIAL_WAIT_TIME;
	private long messageCount = 0;
	private long blankCount = 0;
	private String streamName;
	private boolean debug = false;

	private Reader reader;
	private FileObject fileObject;
	private boolean tailing = true;

	private List<String> csvHeaders;
	private boolean first = true;
	
	private enum InputType {
		CSV,
		JSON,
		TEXT
	}
	private String textColName = "line";
	private InputType inputType = InputType.TEXT;
	
	
	private LinkedBlockingQueue<String> messageQueue = new LinkedBlockingQueue<String>();
	private Set<io.s4.listener.EventHandler> handlers = new HashSet<io.s4.listener.EventHandler>();
	private String delimiter = ";";

	public VfsFileReader(Properties props) {
		if (props != null) {
			if (props.getProperty("file") != null) {
				file = props.getProperty("file");
			}
			if (props.getProperty("columnName") != null) {
				textColName = props.getProperty("columnName");
			}
			if (props.getProperty("maxBackoffTime") != null) {
				maxBackoffTime = Long.parseLong(props.getProperty("maxBackoffTime"));
			}
			if (props.getProperty("stream") != null) {
				streamName = props.getProperty("stream");
				if (streamName.trim().startsWith("debug")) {
					debug = true;
				}
			}
			if (props.getProperty("tail") != null) {
				tailing = Boolean.parseBoolean(props.getProperty("tail"));
			}
			if (props.getProperty("debug") != null) {
				debug = Boolean.parseBoolean(props.getProperty("debug"));
			}

			if (props.getProperty("type") != null) {
				setInputType(props.getProperty("type"));
			}
			if (props.getProperty("delimiter") != null) {
				delimiter = props.getProperty("delimiter");
			}

		}
	}

	public void setlogFile(String urlString) {
		this.file = urlString;
	}

	public void setMaxBackoffTime(long maxBackoffTime) {
		this.maxBackoffTime = maxBackoffTime;
	}

	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}

	public void setTailing(boolean tail) {
		this.tailing = tail;
	}

	public boolean isTailing() {
		return tailing;
	}
	
	private boolean isDebug() {
		return debug;
	}

	public long getWaitMillis() {
		long wait = backOffTime;
		backOffTime = backOffTime * 2;
		if (backOffTime > maxBackoffTime) {
			backOffTime = maxBackoffTime;
		}
		return wait;
	}

	public void setInputType(String type){
		try {
			this.inputType = InputType.valueOf(type);
		}
		catch (Exception e) {
			String values = "";
			for (InputType t : InputType.values()) {
				values += "[" + t + "]";
			}
			Logger.getLogger("s4").error("Unknown output type for FilePersister: " + type 
					+ " Possible values are: " + values);
		}
	}

	public void setDelimiter(String delimiter) {
		this.inputType = InputType.CSV;
		this.delimiter = delimiter;
	}

	
	public void init() {
		//	        for (int i = 0; i < 1; i++) {
		Dequeuer dequeuer = new Dequeuer(1);
		Thread t = new Thread(dequeuer);
		t.start();
		//	        }
		(new Thread(this)).start();
	}

	private void process(BufferedReader br) throws IOException {
		String inputLine = null;
		while ((inputLine = br.readLine()) != null) {
			if (inputLine.trim().length() == 0) {
				blankCount++;
				continue;
			}
			if (first && inputType.equals(InputType.CSV)) {
				csvHeaders = PojoUtil.getCsvColumns(inputLine, delimiter);
				first = false;
				continue;
			}
			first = false;
			messageQueue.add(inputLine);
			messageCount++;
		}
	}
	public void run() {
		do {
			long lastFilePointer = 0;
			long lastFileSize = 0;
			try {
				do {
					FileSystemManager fileSystemManager = VFS.getManager();

					//fileobject was created above, release it and construct a new one
					if (fileObject != null) {
						fileObject.close();
						fileObject = null;
					}
					fileObject = fileSystemManager.resolveFile(file);
					if (fileObject == null) {
						throw new IOException("File does not exist: " + file);
					}
					//file may not exist..
					boolean fileLarger = false;
					if (fileObject != null && fileObject.exists()) {
						try {
							fileObject.refresh();
						} catch (Error err) {
							System.err.println(file + " - unable to refresh fileobject\n");
							err.printStackTrace();
						}
						//could have been truncated or appended to (don't do anything if same size)
						if (fileObject.getContent().getSize() < lastFileSize) {
							reader = new InputStreamReader(fileObject.getContent().getInputStream());
							System.out.println(file + " was truncated");
							lastFileSize = 0;
							lastFilePointer = 0;
							backOffTime = INITIAL_WAIT_TIME;
						} else if (fileObject.getContent().getSize() > lastFileSize) {
							FileContent fc = fileObject.getContent();
							fileLarger = true;
							try {
								RandomAccessContent rac = fc.getRandomAccessContent(RandomAccessMode.READ);
								rac.seek(lastFilePointer);
								reader = new InputStreamReader(rac.getInputStream());
								BufferedReader bufferedReader = new BufferedReader(reader);
								process(bufferedReader);
								lastFilePointer = rac.getFilePointer();
								lastFileSize = fileObject.getContent().getSize();
								rac.close();
							}
							catch (FileSystemException e) {
								// the file doesn't support random access, so let's just read it once
								reader = new InputStreamReader(fc.getInputStream());
								BufferedReader bufferedReader = new BufferedReader(reader);
								process(bufferedReader);
								lastFileSize = fileObject.getContent().getSize();
								fc.close();
							}
							
						}
						try {
							//release file so it can be externally deleted/renamed if necessary
							fileObject.close();
							fileObject = null;
						}
						catch (IOException e)
						{
							System.err.println(file + " - unable to close fileobject\n");
							e.printStackTrace();
						}
						try {
							if (reader != null) {
								reader.close();
								reader = null;
							}
						} catch (IOException ioe) {
							System.err.println(file + " - unable to close reader\n");
							ioe.printStackTrace();
						}
					} else {
						System.out.println(file + " - not available - will re-attempt to load after waiting " + getWaitMillis() + " millis");
					}

					try {
						synchronized (this) {
							wait(getWaitMillis());
						}
					} catch (InterruptedException ie) {}
					if (isTailing() && fileLarger) {
						System.out.println(file + " - tailing file - file size: " + lastFileSize);
					}
				} while (isTailing() && !Thread.interrupted());
			} catch (IOException ioe) {
				System.err.println(file + " - exception processing file\n");
				ioe.printStackTrace();
				try {
					if (fileObject != null) {
						fileObject.close();
					}
				} catch (FileSystemException e) {
					System.err.println(file + " - exception processing file\n");
					e.printStackTrace();
				}
				try {
					synchronized(this) {
						wait(getWaitMillis());
					}
				} catch (InterruptedException ie) {}
			}
		}while(!Thread.interrupted());

		System.out.println(file + " - processing complete");
	}


	class Dequeuer implements Runnable {
		private int id;


		public Dequeuer(int id) {
			this.id = id;
		}

		public void run() {
			while (!Thread.interrupted()) {
				try {
					StreamRow row = null;
					String line = messageQueue.take();
					switch(inputType) {
					case CSV:
						List<String> values = PojoUtil.getCsvColumns(line, delimiter);
						row = PojoUtil.combineStringValues(csvHeaders, values);
						break;
					case JSON:
						row = PojoUtil.fromJson(line);
						break;
					// in case its text we just set a column called line of type string, which is also default
					case TEXT:
						row = new StreamRow();
						row.set(textColName , line, ValueType.STRING);
						break;
					}
					first = false;
					if (isDebug()) {
						System.out.println(row);
					}
					
					if (row == null) {
						return;
					}
					EventWrapper ew = new EventWrapper(streamName, row, null);
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
