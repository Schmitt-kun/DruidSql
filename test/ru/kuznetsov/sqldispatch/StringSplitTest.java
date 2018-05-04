 package ru.kuznetsov.sqldispatch;

import static org.junit.Assert.*;

import org.junit.Test;

public class StringSplitTest {

	@Test
	public void test() {
		String str = "1 2";
		assertArrayEquals(new String[] {"1", "2"}, str.split(" "));
	}

	@Test
	public void test1() {
		String str = "1 2";
		assertArrayEquals(new String[] {"1", "2"}, str.split("[ ]"));
	}
	
	@Test
	public void test2() {
		String str = "1 2";
		assertArrayEquals(new String[] {"1", "2"}, str.split("[ \n]"));
	}
	
	@Test
	public void test3() {
		String str = "1  \n 2";
		assertArrayEquals(new String[] {"1", "2"}, str.split("[ \n\t]+"));
	}
	
	@Test
	public void testComma() {
		sop("1, 2".split("[ \n\t]+"));
		sop("1 , 2".split("[ \n\t]+"));
		sop("1,2".split("[ \n\t]+"));
		
		//assertArrayEquals(new String[] {"1", "2"}, str.split("[ \n\t]+"));
	}
	
	private void sop(String[] arr) {
		System.out.print("[\"");
		for(int  i = 0; i < arr.length; i++) {
			if (i != 0) System.out.print("\", \"");
			System.out.print(arr[i]);
		}
		System.out.println("\"]");
	}
}
