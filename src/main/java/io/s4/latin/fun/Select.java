package io.s4.latin.fun;

import io.s4.latin.pojo.StreamRow;

public class Select {

	private String[] fields;

	public Select(String... fields) {
		this.fields = fields;
	}

	public StreamRow process(StreamRow row) {
		if (!(fields != null && fields.length == 1 && fields[0].equals("*"))) {
			return process(row,fields);
		}
		else {
			return row;
		}
	}

	public static StreamRow process(StreamRow row, String... fields) {
		if (!(fields != null && fields.length == 1 && fields[0].equals("*"))) {
			StreamRow rowNew = new StreamRow();
			for (String key : fields) {
				rowNew.set(key, row.get(key), row.getValueMeta(key));
			}
			return rowNew;
		}
		return row;
	}

	@Override
	public String toString() {
		String str = null;
		if (fields != null ) {
			str =" Select(";
			boolean has = false;
			for (String field : fields) {
				str += field + ", ";
				has = true;
			}
			if (has) {
				str = str.substring(0,str.length()-2);	
			}
			str += ")";
		}
		return str;
	}

}
