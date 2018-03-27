package ru.kuznetsov.sqldispatch;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import ru.kuznetsov.sqldispatch.SqlFilter.FilterType;
import ru.kuznetsov.sqldispatch.SqlFilterGroup.FilterGroupType;

public class SqlDispatch {

	private List<String> metrics;
	private List<String> dimensions;
	
	private String dataSource;
	private List<String> queryDimensions = new ArrayList<>();
	private List<String> queryMetrics = new ArrayList<>();
	private Map<String, SqlFilter> filters = new HashMap<>();
	private SqlFilter filter;
	private SqlFilterGroup rootGroup;
	
	private List<String> words;
	private Integer wordsCounter;
	
	public SqlDispatch(List<String> metrics, List<String> dimensions) {
		this.metrics = metrics;
		this.dimensions = dimensions;
	}
	
	public List<String> getQueryDimensions() {
		return queryDimensions;
	}
	
	public List<String>  getQueryMetrics() {
		return queryMetrics;
	}
	
	public String getDataSource() {
		return dataSource;
	}
	
	public SqlFilter getSqlFilter() {
		return filter;
	}
	
	public SqlFilterGroup getRootSqlFilterGroup() {
		return rootGroup;
	}
	
	public void dispatch(String query) throws SQLException {
		words = new LinkedList<>();
		wordsCounter = 0;
		
		splitQuery(query);
		
		select();
		fields();
		if(dataSource())
			where();
	}
	
	public void splitQuery(String query) {
		Integer lastWordBegin = null;
		if (words == null) words = new ArrayList<>();
		
		for (int i = 0; i < query.length(); i++) {
			if (query.charAt(i) == ' ' 
					|| query.charAt(i) == '\n' 
					|| query.charAt(i) == '\t') {
				if (lastWordBegin != null) {
					String word = query.substring(lastWordBegin, i);
					words.add(word);
					lastWordBegin = null;
				}
			} else if (query.charAt(i) == ','
					|| query.charAt(i) == '('
					|| query.charAt(i) == ')') {
				if (lastWordBegin != null) {
					String word = query.substring(lastWordBegin, i);
					words.add(word);
					lastWordBegin = null;
				}
				words.add("" + query.charAt(i));
			} else {
				if(lastWordBegin == null)
					lastWordBegin = i;
			}
		}
		
		if (lastWordBegin != null) {
			String word = query.substring(lastWordBegin, query.length());
			words.add(word);
		}
	}
	
	private void select() throws SQLException {
		if ("SELECT".equals(words.get(0).toUpperCase())) {
			wordsCounter++;
		} else {
			new SQLException("Expecteed SELECT statement but was \"" + words.get(0) + "\"");
		}
	}
	
	private void fields() throws SQLException {
		boolean wasComma = false;
		boolean first = true;
		while(wordsCounter < words.size()) {
			String currentWord = words.get(wordsCounter++);
			if("FROM".equals(currentWord.toUpperCase())) {
				if (first) throw new SQLException("After SELECT expected field name but was FROM statment");
				if (wasComma) throw new SQLException("After \',\' expected field name but was FROM statment");
				return;
			}
			
			if(",".equals(currentWord)) {
				if (first) throw new SQLException("After SELECT expected field name but was \',\' symbol");
				if (wasComma) throw new SQLException("After \',\' expected field name but was \',\' symbol");
				wasComma = true;
				continue;
			}
			
			
			if (!first && !wasComma)  throw new SQLException("Expected \',\' but was \"" + currentWord + "\"");
			first = false;
			
			wasComma = false;
			if (metrics.contains(currentWord)) queryMetrics.add(currentWord);
			else if (dimensions.contains(currentWord)) queryDimensions.add(currentWord);
			else throw new SQLException("Unknown fild name\"" + currentWord + "\"");
		}
		
		new SQLException("Expecteed FROM statement but was end of query");
	}
	
	private boolean dataSource() throws SQLException {
		dataSource = words.get(wordsCounter++);
		if (wordsCounter == words.size())
			return false;
		
		String word = words.get(wordsCounter++);
		if("WHERE".equals(word.toUpperCase()))
			return true;
		
		throw new SQLException("Expected WHERE statement but was \"" + word + "\"");
	}
	
	private void where() throws SQLException {
		SqlFilterGroup currentGroup = null;
		while(wordsCounter < words.size()) {
			String str = nextWord();
			FilterGroupType typeGroup = null;
			
			if ("AND".equals(str)) {
				typeGroup = FilterGroupType.AND;
			} else if ("OR".equals(str)) {
				typeGroup = FilterGroupType.OR;
			}
			String field = (typeGroup != null) ? nextWord() : str;	
			String op = nextWord();
			String val = nextWord();
			
			FilterType type = SqlFilter.findFiltwerType(op);
			if (type != FilterType.in) {
				filter = new SqlFilter(field, type, val);
			} else {
				if(!"(".equals(val))
					throw new SQLException("After IN expectyed \"(\", but was \"" + val + "\"");
				
				List<String> inFields = new ArrayList<>();
				do {
					val = nextWord();
					inFields.add(val);
					val = nextWord();
				} while(!")".equals(val));
				
				String[] inFieldsArray = inFields.toArray(new String[]{});
				filter = new SqlFilter(field, type, inFieldsArray);
			}
			if (typeGroup != null) {
				SqlFilterGroup newFilter = new SqlFilterGroup(filter);
				if (currentGroup == rootGroup)
					rootGroup = currentGroup = new SqlFilterGroup(typeGroup, currentGroup, newFilter);
				else {
					SqlFilterGroup group = new SqlFilterGroup(typeGroup, currentGroup.getRightOp(), newFilter); 
					currentGroup.setRightOp(group);
					currentGroup = group;
				}
			} else {
				rootGroup = currentGroup =  new SqlFilterGroup(filter);
			}
			//filters.put(field, filter);
		}
	}
	
	private String nextWord() throws SQLException {
		if (wordsCounter > words.size()) throw new SQLException("Unexpected end of query");
		return words.get(wordsCounter++);
	}
	
	public String[] getWords() {
		return words.toArray(new String[0]);
	}
	
	public String toGson() {
		JsonObject query = new JsonObject();
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		
		query.addProperty("queryType", "groupBy");
		query.addProperty("dataSource", dataSource);
		query.addProperty("granularity", "day");
		JsonArray dims = new JsonArray();
		for(String dim : queryDimensions) dims.add(dim);
		query.add("dimensions", dims);
		
		JsonArray aggrs = new JsonArray();
		for(String aggr : queryMetrics) {
			JsonObject aggrObject = new JsonObject();
			aggrObject.addProperty("type", "longSum");
			aggrObject.addProperty("name", aggr);
			aggrObject.addProperty("fieldName", aggr);
			aggrs.add(aggrObject);
		}
		query.add("aggregations", aggrs);
		
		SimpleDateFormat sdf = new SimpleDateFormat("YYYY-mm-dd'T'HH:mm:ss.SSS");
		JsonArray intervals = new JsonArray();
		intervals.add("1970-01-01T00:00:00.000/" + sdf.format(new Date()));
		query.add("intervals", intervals);
		
		if(rootGroup != null) {
			query.addProperty("filter", filterGroupsToJson(rootGroup));
		}
		
		return gson.toJson(query);
	}
	
	public String filterGroupsToJson(SqlFilterGroup group) {
		
		switch (group.getType()) {
		case SINGLE:
			Gson gson = new GsonBuilder().setPrettyPrinting().create();
			JsonObject object = new JsonObject();
			object.addProperty("type", "selector");
			object.addProperty("dimension", group.getFilter().getLeftOp());
			object.addProperty("value", group.getFilter().getRightOp());
			return gson.toJson(object);
		case AND:
		case OR:
			JsonParser parser = new JsonParser();
			JsonObject object1 = new JsonObject();
			object1.addProperty("type", group.getType().name());
			JsonArray array = new JsonArray();
			array.add(parser.parse(filterGroupsToJson(group.getLeftOp())));
			array.add(parser.parse(filterGroupsToJson(group.getRightOp())));
			object1.add("fields", array);
			return object1.toString();
		}
		return "\"\"";
	}
}
