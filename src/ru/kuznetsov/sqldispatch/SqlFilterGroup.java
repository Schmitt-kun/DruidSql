package ru.kuznetsov.sqldispatch;

public class SqlFilterGroup {
	private FilterGroupType type;
	private SqlFilterGroup leftOp;
	private SqlFilterGroup rightOp;
	private SqlFilter filter;
	
	public enum FilterGroupType {AND, OR, SINGLE};
	
	public SqlFilterGroup(FilterGroupType type, SqlFilterGroup leftOp, SqlFilterGroup rightOp) {
		if (type == FilterGroupType.SINGLE) throw new IllegalArgumentException();
		this.type = type;
		this.leftOp = leftOp;
		this.rightOp = rightOp;
		filter = null;
	}
	
	public SqlFilterGroup(SqlFilter filter) {
		type = FilterGroupType.SINGLE;
		this.filter = filter;
	}
	
	public FilterGroupType getType() {
		return type;
	}

	public SqlFilterGroup getLeftOp() {
		return leftOp;
	}

	public SqlFilterGroup getRightOp() {
		return rightOp;
	}
	
	public void setRightOp(SqlFilterGroup rightOp) {
		this.rightOp = rightOp; 
	}
	
	public SqlFilter getFilter() {
		return filter;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SqlFilterGroup other = (SqlFilterGroup) obj;
		
		if (type != other.type)
			return false;
		
		if (type == FilterGroupType.SINGLE)
			if (filter != null)
				return filter.equals(other.filter);
			else
				return other.filter == null;
		
		if (leftOp != null) {
			if (!leftOp.equals(other.leftOp))
				return false;
		} else {
			if (other.leftOp != null) return false;
		}
		
		if (rightOp != null) {
			if (!rightOp.equals(other.rightOp))
				return false;
		} else {
			if (other.rightOp != null) return false;
		}
		
		return true;
	}
	
	public String print() {
		if (type == FilterGroupType.SINGLE)
			return filter.toString();
		return "( " + (type == FilterGroupType.AND ? "AND" : "OR") 
			+ " " + leftOp.print() + "," + rightOp.print() + " )";
	}
}
