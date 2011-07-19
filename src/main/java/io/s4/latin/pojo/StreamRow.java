package io.s4.latin.pojo;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class StreamRow {

	private Map<String,Object> row = new HashMap<String,Object>();
	private Map<String,ValueType> rowMeta = new HashMap<String,ValueType>();
	private String key = "";
	
	public enum ValueType {
		STRING,
		INTEGER,
		NUMBER,
		DATE,
		BOOLEAN,
		OBJECT
	}
	
	public Set<String> getKeys() {
		return row.keySet();
	}
	public void set(String key, Object value, ValueType type) {
		rowMeta.put(key, type);
		row.put(key, value);
	}
	
	public Object get(String key) {
		return row.get(key);
	}
	
	public Collection<Object> getValues() {
		return row.values();
	}
	
	public void remove(String key) {
		row.remove(key);
		rowMeta.remove(key);
	}
	
	
	public ValueType getValueMeta(String key) {
		return rowMeta.get(key);
	}
	
	public void setKey(String... key) {
		this.key = "";
		for (String k : key) {
			this.key += row.get(k);
		}
	}
	
	public String getKey() {
		return this.key;
	}

	@Override
	public String toString() {
		String output = this.getClass().getName() + "[ ";
		for (String key : row.keySet()) {
			output += "(key=\"" + key + "\", value=\"" + row.get(key) + "\")";
		}
		output += " ]";
		return output;
	}
}
