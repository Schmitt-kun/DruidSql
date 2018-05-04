package ru.kuznetsov.sqldispatch;

import java.sql.SQLException;

public class SqlFilter {
	private FilterType type;
	private String leftOp;
	private String rightOp;
	private String[] rightOpArr;

	public enum FilterType {
		Equals("="), Greater(">"), Less("<"), NotEquals("<>"), Like("LIKE"), in("IN");
		
		String operator;
		FilterType(String operator) {
			this.operator = operator;
		}
		String getOperator() {
			return operator;
		}
	}
	
	public FilterType getType () {
		return type;
	}
	
	public String getLeftOp () {
		return leftOp;
	}
	
	public String getRightOp () {
		return rightOp;
	}
	
	public String[] getRightOpArr() {
		return rightOpArr;
	}

	public SqlFilter(String leftOp, FilterType type, String rightOp) {
		this.type = type;
		this.leftOp = leftOp;
		this.rightOp = rightOp;
	}
	
	public SqlFilter(String leftOp, FilterType type, String rightOp, String rightOp1) {
		/*if (type != FilterType.Between)
			throw new IllegalArgumentException("Using constructor with multiple values nor for BETWEEN filter.");*/
		this.type = type;
		this.leftOp = leftOp;
		this.rightOp = rightOp;
	}
	
	public SqlFilter(String leftOp, FilterType type, String[] rightOpArr) {
		/*if (type != FilterType.Between)
			throw new IllegalArgumentException("Using constructor with multiple values nor for BETWEEN filter.");*/
		this.type = type;
		this.leftOp = leftOp;
		this.rightOpArr = rightOpArr;
	}
	
	public static FilterType findFiltwerType(String operator) throws SQLException {
		switch (operator.toUpperCase()) {
		case "=":
			return FilterType.Equals;
		case ">":
			return FilterType.Greater;
		case "<":
			return FilterType.Less;
		case "<>":
			return FilterType.NotEquals;
		case "LIKE":
			return FilterType.Like;
		case "IN":
			return FilterType.in;
		default:
			throw new SQLException("Unknown operator: \"" + operator + "\"");
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((leftOp == null) ? 0 : leftOp.hashCode());
		result = prime * result + ((rightOp == null) ? 0 : rightOp.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SqlFilter other = (SqlFilter) obj;
		if (leftOp == null) {
			if (other.leftOp != null)
				return false;
		} else if (!leftOp.equals(other.leftOp))
			return false;
		
		if (type != other.type)
			return false;
		if (type != FilterType.in) {
			if (rightOp == null) {
				if (other.rightOp != null)
					return false;
			} else if (!rightOp.equals(other.rightOp))
				return false;
		} else {
			if (rightOpArr == null) {
				return other.rightOpArr == null;
			} else {
				if (rightOpArr.length != other.rightOpArr.length)
					return false;
				for(int i = 0 ; i < rightOpArr.length; i++) {
					if (rightOpArr[i] == null) {
						if(other.rightOpArr[i] == null)
							continue;
						else
							return false;
					}
					if (!rightOpArr[i].equals(other.rightOpArr[i]))
						return false;
				}
			}
		}
		
		return true;
	}
	
	@Override
	public String toString() {
		return  leftOp + " " + type.getOperator() + " " + rightOp;
	}
}
