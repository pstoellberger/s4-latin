package io.s4.latin.pojo;

import io.s4.latin.pojo.StreamRow.ValueType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class PojoUtil {

	public static JSONObject toJson(StreamRow row) throws Exception {
		JSONObject jsonRow = new JSONObject();
		for (String key : row.getKeys()) {
			if (row.get(key) != null) {
				jsonRow.put(key, row.get(key));
			}
		}
		return jsonRow;
	}

	public static JSONArray toJsonArray(Collection<StreamRow> rows) throws Exception {
		JSONArray jsonArray = new JSONArray();
		for (StreamRow row : rows) {
			JSONObject jrow = toJson(row);
			jsonArray.put(jrow);
		}
		return jsonArray;

	}

	public static String getCsvHeader(StreamRow row, String delimiter) {
		return toCsv(row.getKeys(),delimiter);
	}
	
	public static List<String> getCsvColumns(String row, String delimiter) {
		String[] hs = row.split(delimiter);
		List<String> headers = new ArrayList<String>(Arrays.asList(hs));
		return headers;
	}
	
	public static StreamRow combine(List<String> keys, List<Object> values, List<ValueType> types) {
		if (keys.size() != values.size() || values.size() != types.size()) {
			throw new IllegalArgumentException("Cannot combine unequal number of keys ("+ keys.size()
					+ ") values ("+ values.size() + ") types ("+ types.size() + ")");
		}
		StreamRow row = new StreamRow();
		for (int i = 0; i<keys.size();i++) {
			row.set(keys.get(i), values.get(i), types.get(i));
		}
		return row;
	}
	
	public static StreamRow combineStringValues(List<String> keys, List<String> values) {
		if (keys.size() != values.size()) {
			throw new IllegalArgumentException("Cannot combine unequal number of keys ("+ keys.size()
					+ ") values ("+ values.size() + ")");
		}
		StreamRow row = new StreamRow();
		for (int i = 0; i<keys.size();i++) {
			row.set(keys.get(i), values.get(i), ValueType.STRING);
		}
		return row;
	}

	public static String toCsvRow(StreamRow row, String delimiter) {
		return toCsv(row.getValues(),delimiter);
	}

	public static String toCsv(Collection<?> values, String delimiter) {
		String line = "";
		boolean first = true;
		for (Object value : values) {
			if (!first) {
				line += delimiter;
			}
			line += value;
			first = false;
		}
		line += "\n";
		return line;

	}

	public static StreamRow fromJson(String json) {
		try {
			JSONObject jsonRow = new JSONObject(json);
			StreamRow row = new StreamRow();
			for (String name : JSONObject.getNames(jsonRow)) {
				Object obj = jsonRow.get(name);
				row.set(name, obj, getType(obj));
			}
			return row;
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static ValueType getType(Object o) {
		if (o instanceof String) {
			return ValueType.STRING;
		}
		if (o instanceof Integer) {
			return ValueType.INTEGER;
		}
		if (o instanceof Date) {
			return ValueType.DATE;
		}
		if (o instanceof Number) {
			return ValueType.NUMBER;
		}
		if (o instanceof Boolean) {
			return ValueType.BOOLEAN;
		}
		return ValueType.OBJECT;
	}
}
