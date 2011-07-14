package io.s4.latin.pojo;

import java.util.Collection;

import org.json.JSONArray;
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
}
