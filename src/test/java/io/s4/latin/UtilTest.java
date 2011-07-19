package io.s4.latin;

import io.s4.latin.fun.Where;
import io.s4.latin.pojo.StreamRow;
import io.s4.latin.pojo.StreamRow.ValueType;
import junit.framework.TestCase;

public class UtilTest extends TestCase {
	
	public UtilTest() {
		super();
	}
	
	public void testWhere() {
		StreamRow row = new StreamRow();

		String condition = "\"speaker\" = 'franklin delano roosevelt'";
		assertFalse(Where.checkCondition(row, condition));
		assertNull(Where.process(row, condition));

		condition = "\"speaker\"='franklin delano roosevelt'";
		assertFalse(Where.checkCondition(row, condition));
		assertNull(Where.process(row, condition));
		
		condition = " \"speaker\" != 'franklin delano roosevelt'";
		assertTrue(Where.checkCondition(row, condition));	
		assertNotNull(Where.process(row, condition));
		
		row.set("speaker", "franklin delano roosevelt", ValueType.STRING);
		condition = " \"speaker\" != 'franklin delano roosevelt'";
		assertFalse(Where.checkCondition(row, condition));
		assertNull(Where.process(row, condition));
		
		condition =  " \"speaker\" = 'franklin delano roosevelt' or \"speaker\" != 'mr nixon'";
		assertNotNull(Where.process(row, condition));
		
		condition =  " \"speaker\" = 'franklin delano roosevelt' or \"speaker\" = 'mr nixon'";
		assertNotNull(Where.process(row, condition));
		
		condition =  " \"speaker\" = 'franklin delano roosevelt' and \"speaker\" = 'mr nixon'";
		assertNull(Where.process(row, condition));
		

		condition = " \"speaker\" = 'franklin delano roosevelt'";
		assertTrue(Where.checkCondition(row, condition));
		assertNotNull(Where.process(row, condition));
		
		
		row.set("speaker", "frankling delano roosevelt", ValueType.STRING);
		condition = "\"speaker\" = 'franklin delano roosevelt'";
		assertFalse(Where.checkCondition(row, condition));
		assertNull(Where.process(row, condition));
		
		condition = "\"speaker\" != 'franklin delano roosevelt'";
		assertTrue(Where.checkCondition(row, condition));
		assertNotNull(Where.process(row, condition));
		
		
		assertTrue(Where.checkCondition(row, "'123' = '123'"));
		assertNotNull(Where.process(row, condition));
		
		assertTrue(Where.checkCondition(row, "'123' < '1234'"));
		assertNotNull(Where.process(row, condition));
		
		assertTrue(Where.checkCondition(row, "'1234' > '123'"));
		assertNotNull(Where.process(row, condition));
		
		assertTrue(Where.checkCondition(row, "'1234.0' > '123.0'"));
		assertNotNull(Where.process(row, condition));
		
		assertTrue(Where.checkCondition(row, "'123.0' < '1234.0'"));
		assertNotNull(Where.process(row, condition));
		
		assertTrue(Where.checkCondition(row, "'123.0' = '123.0'"));
		assertNotNull(Where.process(row, condition));
		
		
		row = new StreamRow();
		row.set("greater", 1234, ValueType.NUMBER);
		row.set("smaller", 123, ValueType.NUMBER);
		
		condition =  "\"smaller\" = \"greater\"";
		assertFalse(Where.checkCondition(row, condition));
		assertNull(Where.process(row, condition));
		
		condition = "\"smaller\" <= \"greater\"";
		assertTrue(Where.checkCondition(row, condition));
		assertNotNull(Where.process(row, condition));
		
		condition = "\"smaller\" < \"greater\"";
		assertTrue(Where.checkCondition(row, condition));
		assertNotNull(Where.process(row, condition));
		
		condition = "\"smaller\" > \"greater\"";
		assertFalse(Where.checkCondition(row,condition));
		assertNull(Where.process(row, condition));
		
		condition = "\"smaller\" >= \"greater\"";
		assertFalse(Where.checkCondition(row,condition));
		assertNull(Where.process(row, condition));
		
		condition = "\"smaller\" < '12345'";
		assertTrue(Where.checkCondition(row, condition));
		assertNotNull(Where.process(row, condition));
		
		condition = "'1234.5' > \"greater\"";
		assertTrue(Where.checkCondition(row, condition));
		assertNotNull(Where.process(row, condition));
		
	}
}
