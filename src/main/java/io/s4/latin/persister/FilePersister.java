package io.s4.latin.persister;

import io.s4.latin.pojo.PojoUtil;
import io.s4.latin.pojo.StreamRow;
import io.s4.persist.Persister;

import java.io.FileWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONObject;

public class FilePersister implements Persister {

	private String outputFilename;
	private int persistCount = 0;
	private boolean first = true;
	private String delimiter = "\t";

	private enum OutputType {
		CSV,
		JSON
	}

	private OutputType outputType = OutputType.CSV;

	public FilePersister() {
		// TODO Auto-generated constructor stub
	}

	public FilePersister(Properties props) {
		if (props != null) {
			if (props.getProperty("type") != null) {
				setOutputType(props.getProperty("type"));
			}
			if (props.getProperty("file") != null) {
				outputFilename = props.getProperty("file");
			}
			if (props.getProperty("delimiter") != null) {
				delimiter = props.getProperty("delimiter");
			}
		}
	}

	public void setOutputFilename(String outputFilename) {
		this.outputFilename = outputFilename;
	}

	public void setOutputType(String type){
		try {
			this.outputType = OutputType.valueOf(type);
		}
		catch (Exception e) {
			String values = "";
			for (OutputType t : OutputType.values()) {
				values += "[" + t + "]";
			}
			Logger.getLogger("s4").error("Unknown output type for FilePersister: " + type 
					+ " Possible values are: " + values);
		}
	}

	public void setDelimiter(String delimiter) {
		this.outputType = OutputType.CSV;
		this.delimiter = delimiter;
	}

	@Override
	public int cleanOutGarbage() throws InterruptedException {
		return 0;
	}

	@Override
	public Object get(String arg0) throws InterruptedException {
		return null;
	}

	@Override
	public Map<String, Object> getBulk(String[] arg0)
	throws InterruptedException {
		return new HashMap<String, Object>();
	}

	@Override
	public Map<String, Object> getBulkObjects(String[] arg0)
	throws InterruptedException {
		return new HashMap<String, Object>();
	}

	@Override
	public int getCacheEntryCount() {
		return 1;
	}

	@Override
	public Object getObject(String arg0) throws InterruptedException {
		return null;
	}

	@Override
	public int getPersistCount() {
		return persistCount;
	}

	@Override
	public int getQueueSize() {
		return 0;
	}

	@Override
	public Set<String> keySet() {
		return new HashSet<String>();
	}

	@Override
	public void remove(String arg0) throws InterruptedException {

	}

	@Override
	public void set(String key, Object value, int persistTime) throws InterruptedException {
		FileWriter fw = null;

		try {
			if (value == null) 
				throw new IllegalArgumentException("Cannot pass empty value parameter to set()");

			if (!(value instanceof List))
				throw new IllegalArgumentException("You need to pass a List<StreamRow> to set()");


			@SuppressWarnings("unchecked")
			List<Object> v = (List<Object>) value;

			if (v.size() == 0)
				return;

			if (!(v.get(0) instanceof StreamRow)) 
				throw new IllegalArgumentException("You need to pass a List<StreamRow> to set()");


			@SuppressWarnings("unchecked")
			List<StreamRow> rows = (List<StreamRow>) value;
			fw = new FileWriter(outputFilename,!first);

			for (StreamRow row : rows) {

				if (outputType.equals(OutputType.JSON)) {
					JSONObject output = PojoUtil.toJson(row);
					fw.append(output.toString() + "\n");

				} else if (outputType.equals(OutputType.CSV)) {

					if (first) {
						fw.write(PojoUtil.getCsvHeader(row, delimiter));
						first = false;
					}
					fw.append(PojoUtil.toCsv(row.getValues(), delimiter));
				}
				persistCount++;
				if (persistCount % 500 == 0) {
					System.out.println("Lines persisted: " + persistCount );
				}

			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			Logger.getLogger("s4").error(e);
		} finally {
			if (fw != null) {
				try {
					fw.close();
				} catch (Exception e) {
				}
			}
		}


	}

	@Override
	public void setAsynch(String key, Object value, int persistTime) {
		try {
			set(key, value, persistTime);
		} catch (InterruptedException ie) {
		}
	}

}
