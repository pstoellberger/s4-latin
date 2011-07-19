package io.s4.latin.fun;

import io.s4.latin.pojo.StreamRow;

import java.util.ArrayList;
import java.util.List;

public class Where {


	public static StreamRow process(StreamRow row, String conditions) {

		if (conditions != null && conditions.length() > 0) {
			String[] ands = conditions.split(" and ");
			boolean andTrue = true;
			for (String and : ands) {
//				System.err.println("processing and: " + and);
				boolean orTrue = false;
				for (String orstr : and.split(" or ")) {
//					System.err.println("check or: " + orstr);
					orTrue = orTrue | checkCondition(row, orstr);
				}
				andTrue = orTrue && andTrue;

				if (!andTrue) {
					break;
				}
			}

//			System.err.println("andtrue: " + (andTrue));
			if (andTrue) {
				return row;
			}
			else {
				return null;
			}
		} else {
			return row;
		}
	}

	public static boolean checkCondition(StreamRow row, String condition) {
		String currentToken="";
		List<String> tokens = new ArrayList<String>();
		boolean openLiteral = false;
		boolean openOperator = false;
		boolean openColumn = false;
		condition = condition.trim().toLowerCase();
		for (char c : condition.toCharArray()) {
			if (!openLiteral && !openOperator && !openColumn && c != ' ') {
				if (c == '"' && !openColumn) {
					openLiteral = false;
					openColumn = true;
					openOperator = false;
				} else if ((c == '\'') && !openLiteral ) {
					openLiteral = true;
					openColumn = false;
					openOperator = false;
				} else if ((c == '>' || c == '<' || c == '=' || c =='!') && !openOperator) {
					openLiteral = false;
					openColumn = false;
					openOperator = true;
				}
				currentToken += c;
			}else if (openLiteral || openColumn || openOperator) {
				boolean open = true, 
				append = false;
				if (c == '"' && openColumn) {
					open = false;
					currentToken += c;
				} else if (c == '\'' && openLiteral) {
					open = false;
					currentToken += c;
				} else if ((c == ' ' || c == '\'' || c == '"') && openOperator) {
					open = false;
					if ( c == '\'' || c == '"') {
						append = true;
					}
				} else {
					currentToken += c;
				}
				if (!open) {
					openOperator = false;
					openLiteral = false;
					openColumn = false;
					tokens.add(currentToken);
					currentToken = "";
				}
				if (append) {
					currentToken += c;
					openLiteral = true;
				}
			}
		}
		if (currentToken != "") {
			tokens.add(currentToken);
		}
//		System.err.println("######################### TOKENS");
//		for (String token: tokens) {
//			System.err.println("TOKEN: " + token);
//		}
		if (tokens.size() == 3) {
			String left = tokens.get(0);
			String operator = tokens.get(1).trim();
			String right = tokens.get(2);
			Object objLeft = transform(left, row);
			Object objRight = transform(right, row);
//			System.err.println("Condition (" + condition + ") Where (" +objLeft + " "+ operator + " " + objRight + ")");

			if ("=".equals(operator.trim()) || "==".equals(operator.trim())) {
				if (objLeft != null && objRight != null) {
					return objLeft.equals(objRight);
				}
				else {
					if (objLeft == null && objRight == null) {
						return true;
					}
				}
				return false;
			}
			if ("!=".equals(operator.trim()) || "<>".equals(operator.trim())) {
				if (objLeft != null && objRight != null) {
					return !objLeft.equals(objRight);
				}
				return (objLeft == null) ^ (objRight == null);
			}


			if (objLeft instanceof Number && objRight instanceof Number) {
				if (">".equals(operator) && ((Number) objLeft).doubleValue() > ((Number) objRight).doubleValue()) {
					return true;
				}
				if (">=".equals(operator) && ((Number) objLeft).doubleValue() >= ((Number) objRight).doubleValue()) {
					return true;
				}
				if ("<=".equals(operator) && ((Number) objLeft).doubleValue() <= ((Number) objRight).doubleValue()) {
					return true;
				}
				if ("<".equals(operator) && ((Number) objLeft).doubleValue() < ((Number) objRight).doubleValue()) {
					return true;
				}
			}

			return false;

		} else {
			throw new IllegalArgumentException("Condition must consist of 3 tokens (Literal|Field) Operator (Literal|Field)");
		}
	}

	private static Object transform(String token, StreamRow row) {
		Object obj;
		String strObj = null;
		if (token.startsWith("'") && token.endsWith("'")) {
			strObj = token.substring(1,token.length()-1).toLowerCase();
		} else if (token.startsWith("\"") && token.endsWith("\"")) {
			token = token.substring(1,token.length()-1).toLowerCase();
			Object o = row.get(token);
			if (o != null) {
				strObj = o.toString();
			}
		}
		try {
			Integer l = Integer.parseInt(strObj);
			obj = l;
		}
		catch (Exception e) { 
			try {
				Double l = Double.parseDouble(strObj);
				obj = l;
			}
			catch (Exception ef) {
				obj = strObj;
			} 

		}
		return obj;
	}


}
