package io.s4.latin.fun;

import io.s4.latin.pojo.StreamRow;

public class Where {


	public static StreamRow process(StreamRow row, String condition) {

		if (condition != null && condition.length() > 0) {
			String[] ands = condition.split(" and ");
			boolean andTrue = false;
			int andIndex = 0;
			for (String and : ands) {
//				System.out.println("processing and: " + and);
				boolean orTrue = false;
				int orIndex = 0;
				for (String orstr : and.split(" or ")) {
//					System.out.println("check or: " + orstr);
					boolean result = checkCondition(row, orstr);
					orIndex++;
					orTrue = result | orTrue;
//					if (orTrue) {
//						break;
//					}
				}
//				if (and.split("or").length == 0) {
//					orTrue = checkCondition(row, and);
//				}

				boolean result = checkCondition(row, and);
				andTrue = result && andTrue;
				if ((orIndex == 0 || orTrue) && andTrue) {
					break;
				}
			}

//			if (ands.length == 0) {
//				andTrue = checkCondition(row, condition);
//			}
//			System.out.println("andtrue: " + (andTrue || (andIndex == 0)));
			if (andTrue || (andIndex == 0)) {
				return row;
			}
			else {
				return null;
			}
		} else {
			return row;
		}
	}

	private static boolean checkCondition(StreamRow row, String condition) {
		String[] tokens = condition.split(" ");
		if (tokens.length == 3) {
			String left = tokens[0];
			String operator = tokens[1];
			String right = tokens[2];

			Object objLeft = transform(left, row);
			Object objRight = transform(right, row);

			if ("=".equals(operator.trim())) {
//				System.out.println("CHECK CONDITION: " + condition+ " # " + objLeft + " = " + objRight + " =  " + objLeft.equals(objRight));
				if (objLeft != null && objRight != null) {
					return objLeft.equals(objRight);
				}
				else {
					return true;
				}
			}

			return false;

		} else {
			throw new RuntimeException("Condition must consist of 3 tokens (Literal|Field) Operator (Literal|Field)");
		}
	}

	private static Object transform(String token, StreamRow row) {
		Object obj;

		if (token.startsWith("'") && token.endsWith("'")) {
			obj = token.substring(1,token.length()-1).toLowerCase();
		} else if (token.startsWith("\"") && token.endsWith("\"")) {
			token = token.substring(1,token.length()-1).toLowerCase();
			obj = row.get(token);
		} else {
			try {
				Integer l = Integer.parseInt(token);
				obj = l;
			}
			catch (Exception e) { 
				try {
					Double l = Double.parseDouble(token);
					obj = l;
				}
				catch (Exception ef) { 
					obj = token;
				}
			}
		}
		return obj;
	}
}
