package ru.kuznetsov.sqldispatch;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Main {

	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println("Usage SqlDispatch [SqlQuery]");
			return;
		}
		
		if(args.length == 1) {
			System.out.println("\"" + args[0] + "\"");
			List<String> metrics = new ArrayList<>();
			metrics.add("metric");
			metrics.add("metric1");
			List<String> dimensions = new ArrayList<>();
			dimensions.add("dimension");
			dimensions.add("dimension1");
			SqlDispatch sqlDispatch = new SqlDispatch(metrics, dimensions);
			try {
				sqlDispatch.dispatch(args[0]);
				String json = sqlDispatch.toGson();
				System.out.println(json);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
