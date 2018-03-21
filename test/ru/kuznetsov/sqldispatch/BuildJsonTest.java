package ru.kuznetsov.sqldispatch;

import static org.junit.Assert.*;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

public class BuildJsonTest {

	@Test
	public void test() {
		JsonObject json = new JsonObject();
		
		json.addProperty("property", "value");
		json.addProperty("property1", "value");
		
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		System.out.println(gson.toJson(json));
	}

}
