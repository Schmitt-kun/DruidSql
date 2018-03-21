package ru.kuznetsov.sqldispatch;

import static org.junit.Assert.*;

import org.junit.Test;

public class CommaReplaceTest {

	@Test
	public void testA() {
		String str = "a,b";
		assertEquals("a , b", str.replace(",", " , "));
	}

	@Test
	public void testB() {
		String str = "\"\",b";
		
		assertEquals("\"\" , b", str.replaceAll("[\"[^\"]\"]+,", " , "));
	}
}
