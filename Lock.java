package TwoPhaseLocking;

import java.util.PriorityQueue;
import java.util.ArrayList;
import java.util.List;

public class Lock {
	String dataItem;
	String lockState;
	int writeLockTransId;
	List<Integer> readTransactionId;
	PriorityQueue<Integer> waitingList;

	
	public Lock(){
		readTransactionId = new ArrayList<Integer>();
		waitingList = new PriorityQueue<Integer>();
		
	}
	
	public Lock(String dataItem, String lockState,int writeLockTransId){
		this.dataItem=dataItem;
		this.lockState=lockState;
		this.writeLockTransId=writeLockTransId;
		readTransactionId = new ArrayList<Integer>();
		waitingList = new PriorityQueue<Integer>();
	}

	public String getDataItem() {
		return dataItem;
	}

	public void setDataItem(String dataItem) {
		this.dataItem = dataItem;
	}

	public String getLockState() {
		return lockState;
	}

	public void setLockState(String lockState) {
		this.lockState = lockState;
	}

	public int getWriteLockTransId() {
		return writeLockTransId;
	}

	public void setWriteLockTransId(int writeLockTransId) {
		this.writeLockTransId = writeLockTransId;
	}

	public List<Integer> getReadTransactionId() {
		return readTransactionId;
	}

	public void setReadTransactionId(List<Integer> readTransactionId) {
		this.readTransactionId = readTransactionId;
	}


	public PriorityQueue<Integer> getWaitingList() {
		return waitingList;
	}

	public void setWaitingList(PriorityQueue<Integer> waitingList) {
		this.waitingList = waitingList;
	}


	
}
