package TwoPhaseLocking;


public class Operation {
	String operation;
	String dataItem;
	
	public Operation(String operation,String dataItem){
		this.operation = operation;
		this.dataItem=dataItem;
	}
	
	public String getOperation() {
		return operation;
	}
	public void setOperation(String operation) {
		this.operation = operation;
	}
	public String getDataItem() {
		return dataItem;
	}
	public void setDataItem(String dataItem) {
		this.dataItem = dataItem;
	}

}
