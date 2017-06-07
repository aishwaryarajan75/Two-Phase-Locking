package TwoPhaseLocking;


//import java.util.ArrayList;
//import java.util.List;
import java.util.*;


public class Transaction {
	int transId;
	int timestamp;
	String state;
	Queue<String> DataItems;
	Queue<Operation> waitingOperation;
	
	public Transaction(int transId,int timestamp,String state){
		this.transId=transId;
		this.timestamp=timestamp;
		this.state=state;
		DataItems=new LinkedList<String>();
		waitingOperation=new LinkedList<Operation>();
	}
	
	public int getTransId() {
		return transId;
	}
	public void setTransId(int transId) {
		this.transId = transId;
	}
	
	public int getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(int timestamp) {
		this.timestamp = timestamp;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}

}

