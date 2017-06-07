package TwoPhaseLocking;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import TwoPhaseLocking.Lock;
import TwoPhaseLocking.Transaction;

class RigorousTwoPhaseLocking {
	//Transaction table
	static Map<Integer,Transaction> transactionTableMap = new HashMap<Integer,Transaction>();
		
	//Lock table
	static Map<String,Lock> lockTableMap = new HashMap<String,Lock>();
	
	
	 public static void main(String [] args) {
		 String fileName="C:/UTA/Summer2016/Project1/Input.txt";
		 String line1 = null; //reference one line at a time
		 
		 int timestamp = 0;
	
		 try{
			 FileReader fileReader = new FileReader(fileName);		
			 BufferedReader bufferedReader = new BufferedReader(fileReader);
		
			 while ((line1=bufferedReader.readLine())!=null){
				 //operation = line.charAt(0);
				 String line = line1.replace(" ", "");
				 
				 if(line.charAt(0)=='b'){
					 timestamp=timestamp+1;
					 begin(timestamp,Integer.parseInt(line.substring(1, line.indexOf(";"))));
				 }
				 else if(line.charAt(0)=='r' || line.charAt(0)=='w'){
					 
					 request(line.substring(line.indexOf('(')+1,line.indexOf(')')), Integer.parseInt(line.substring(1, line.indexOf('('))), line.charAt(0)+"");
				 }
				 else if(line.charAt(0)=='c'){
					 commit(transactionTableMap.get(Integer.parseInt(line.substring(1, line.indexOf(";")))));
				 }
			 }
			 bufferedReader.close();
		 }
		 catch(FileNotFoundException ex){
			 System.out.println("Unable to open file '" +fileName + "'");		
		 }	
		 catch (IOException e) {		
			 e.printStackTrace();
		 }
	}
	 
	 //begin transaction
	 public static void begin(int timestamp,int transId){
		 Transaction trans= new Transaction( transId,timestamp,"Active");			
		 transactionTableMap.put(transId, trans);
		 System.out.println("Transaction "+transId+ " has begun and it has been entered in the transaction "
		 		+ "table with it's state as Active and timestamp as "+timestamp+".\n" );
	 }
		
	// request method which checks the state of the transactions and then calls the desired functions  
	public static void request(String dataItem, int transID, String op){
		op = op.equals("r") ? "Read" : "Write";
		Transaction t2 = transactionTableMap.get(transID); // Incoming ID
		 
		 //state of the transaction is active then
		 if(t2.state.equals("Active")){
			 active(dataItem,t2, op);
		 } 
		 //state of the transaction is blocked
		 else if(t2.state.equals("Block")){
			 block(dataItem, t2, op);
		 }
		 //state of the transaction is aborted
		 else if(t2.state.equals("Abort")){
			 System.out.println("Transaction "+t2.transId+" is aborted");			 
		 } 
		 //state of the transaction is committed
		 else if(t2.state.equals("Commit")){
			 System.out.println(" transaction "+t2.transId+" is committed");
		 }
	}
 // if the state of transaction is active
	public static void active(String dataItem, Transaction incomming, String op){
		 if(lockTableMap.containsKey(dataItem)){
			 Lock lock = lockTableMap.get(dataItem);
			 if(lock.lockState.equals("Read") && op.equals("Read")){
				 lock = rr(dataItem, incomming, lock);
			 } 
			 else if(lock.lockState.equals("Read") && op.equals("Write")){
				 lock = rw(dataItem, incomming, lock);
			 } 
			 else if(lock.lockState.equals("Write") && op.equals("Read")){
				 lock  = wr(dataItem, incomming, lock);
			 } 
			 else if(lock.lockState.equals("Write") && op.equals("Write")){
				 lock = ww(dataItem, incomming, lock);
			 }  else if(lock.lockState.equals("") && op.equals("Read")){
				 lock.lockState = "Read";
				 lock.readTransactionId.add(incomming.transId);
				 System.out.println("Transaction "+incomming.transId+" has acquired Read Lock on data item "+dataItem);
			 } else if(lock.lockState.equals("") && op.equals("Write")){
				 lock.lockState = "Write";
				 System.out.println("Transaction "+incomming.transId+" has acquired Write Lock on data item "+dataItem);
				 lock.writeLockTransId = incomming.transId;
			 }
			 lockTableMap.put(dataItem,lock);
		 } 
		 else {
			 Lock lock = null;
			 if(op.equals("Read")){
				  lock = new Lock(dataItem,op,0);
				  lock.readTransactionId.add(incomming.transId);
				  
				 System.out.println("The transaction state for transaction "+incomming.transId+" is Active so entry for data item "+dataItem+" has been "
				 		+ "made in the lock table and transaction "+incomming.transId+" has acquired "
				 				+ "Read Lock on it."+"\n");
			 }
			 else if(op.equals("Write")){
				 lock = new Lock(dataItem,op,incomming.transId);
				 System.out.println("The transaction state for transaction "+incomming.transId+" is Active so entry for data item "+dataItem+" has "
				 		+ "been made in the lock table and "+ "transaction "+incomming.transId+" has acquired "
				 				+ "Write Lock on it."+"\n");
			 }
			 
			 if(!incomming.DataItems.contains(dataItem))
				 incomming.DataItems.add(dataItem);
			 transactionTableMap.put(incomming.transId, incomming);
			 lockTableMap.put(dataItem, lock);
			}
			 
	}
	//lock state in lock table is read and the state of incoming transaction is also read
	public static Lock rr(String dataItem, Transaction in, Lock lock){
		 lock.readTransactionId.add(in.transId);
		 if(!in.DataItems.contains(dataItem))
		 in.DataItems.add(dataItem);
		 transactionTableMap.put(in.transId, in);
		 System.out.println("Transaction "+in.transId+" has been appended in the read transaction id list for item "
		 		+dataItem+ ". That is, it has also acquired Read Lock on "+dataItem+".\n");
		 return lock;
	}

	//lock state in lock table is write and the state of incoming transaction is read
	public static Lock wr(String dataItem, Transaction in, Lock lock){
		//transaction Id is same downgrade the lock
		 if(lock.writeLockTransId == in.transId){
			 lock.lockState="Read";
			 lock.writeLockTransId=0;
			 lock.readTransactionId.add(in.transId);
			 if(!in.DataItems.contains(dataItem))
			 in.DataItems.add(dataItem);
			 transactionTableMap.put(in.transId, in);
			 System.out.println("For the data item "+dataItem+" and transaction ID "+in.transId+" lock has been downgraded to Read Lock."+"\n");
		 }
		 else{
			 Transaction t1 = transactionTableMap.get(lock.writeLockTransId); //locktable ID
			 if(t1.timestamp>in.timestamp){
				 t1.state="Abort";
				 transactionTableMap.put(t1.transId,t1);
				 
				 lock.writeLockTransId=0;
				 lock.lockState="Read";
				 lock.readTransactionId.add(in.transId);
				 if(!in.DataItems.contains(dataItem))
					 in.DataItems.add(dataItem);
				 transactionTableMap.put(in.transId, in);
				 
				 System.out.println("Transaction "+t1.transId+" abortes because it has higher timestamp and transaction "
				 +in.transId+" acquires Read Lock on data item "+dataItem+"\n");
				 releaseLock(t1,dataItem);
				 
			 }
			 else{
				 in.state="Block";
				 in.waitingOperation.add(new Operation("Read",dataItem));
											 
				 lock.waitingList.add(in.transId);
				 
				 if(!in.DataItems.contains(dataItem))
					 in.DataItems.add(dataItem);
				 transactionTableMap.put(in.transId, in);
				 System.out.println("Transaction "+in.transId+" has been blocked and Read operation "
				 +dataItem+" has been added to the waiting operation queue in the transaction table and transaction Id "+in.transId+
				 " has been added to the waiting list queue in the lock table."+"\n");
			 }
		 }	
		 
		 return lock;
	}	
	
	//lock state in lock table is read and the state of incoming transaction is write	
	public static Lock rw(String dataItem, Transaction in, Lock lock){
		//upgrade the lock
		if(lock.readTransactionId.size()==1 && lock.readTransactionId.get(0).equals(in.transId)){
			 lock.lockState="Write";
			 lock.readTransactionId.remove(0);
			 lock.writeLockTransId = in.transId;
			 System.out.println("For the data item "+dataItem+" and transaction ID "+in.transId+" lock has been upgraded to Write Lock."+"\n");
		 }
		 else if(lock.readTransactionId.size()==1 && !lock.readTransactionId.get(0).equals(in.transId)){
			 Transaction t1 = transactionTableMap.get(lock.writeLockTransId);//
			 if(t1.timestamp>in.timestamp){
				 t1.state="Abort";
				 transactionTableMap.put(t1.transId, t1);
				 lock.lockState="Write";
				 lock.writeLockTransId=in.transId;
				 lock.readTransactionId.remove(0);
				 System.out.println("Transaction "+t1.transId+" abortes because of higher timestamp and transaction"
				 +in.transId+" acquires Write Lock on data item "+dataItem+".\n");
				 releaseLock(t1,dataItem);
				 
			 }
			 else{
				 in.state = "Block";
				 in.waitingOperation.add(new Operation("Write",dataItem));
				 transactionTableMap.put(in.transId,in);							 
				 lock.waitingList.add(in.transId);
				 System.out.println("Transaction "+in.transId+" is blocked because of higher timestamp and write "
				 		+ "operation for "+dataItem +" has been added to the waiting operation queue of transaction table and the transactio ID"
				 		+in.transId+" has been added to the waiting list queue of lock table."+"\n");
				 }
			 
		 }					 
		 else if(lock.readTransactionId.size()>1){
			 List<Integer> readTransId = lock.readTransactionId;
			 Collections.sort(readTransId);
			 int first = readTransId.get(0);
			 if(first == in.transId){
				 System.out.println("For transaction "+in.transId+" lock for data item "+dataItem+" has been upgraded to write.");
				 for(int i = 1;i<readTransId.size();i++){
					Transaction t1 = transactionTableMap.get(readTransId.get(i));
					abort(t1);
					System.out.println("Aborting transaction "+t1.transId);
				 }
				 lock.readTransactionId.clear();
				 lock.writeLockTransId = first;
			 } else if(in.transId < first){
				 System.out.println("Transaction "+in.transId+" has aquired write lock for data item "+dataItem);

				 for(int i = 0;i<readTransId.size();i++){
						Transaction t1 = transactionTableMap.get(readTransId.get(i));
						abort(t1);
						System.out.println("Aborting transaction "+t1.transId);
					 }
				 lock.readTransactionId.clear();
				 lock.writeLockTransId = first;
			 } else {
				 in.state = "Block";
				 in.waitingOperation.add(new Operation(dataItem, "Write"));
				 System.out.println("transaction "+in.transId+" has been blocked because it has higher timestamp.");
				 int i = 0;
				 while(i<readTransId.size()){
					    if(in.transId >= readTransId.get(i)) i++;
					    if(in.transId < readTransId.get(i)) {
					    	readTransId.remove(i);
					    	Transaction t1 = transactionTableMap.get(readTransId.get(i));
							abort(t1);
							System.out.println("Aborting transaction "+t1.transId);
					    }	
				  }	 
				 lock.readTransactionId = readTransId;
			 }

				 
		 }
		if(!in.DataItems.contains(dataItem)){
		 in.DataItems.add(dataItem);
		 transactionTableMap.put(in.transId, in);
		}
		 return lock;
	}
	
	//lock state in lock table is write and the state of incoming transaction is also write
	public static Lock ww(String dataItem, Transaction in, Lock lock){
		 Transaction t1 = transactionTableMap.get(lock.writeLockTransId);
		 if(t1.timestamp>in.timestamp){
			 t1.state="Abort";
			 transactionTableMap.put(t1.transId, t1);
			 lock.writeLockTransId=in.transId;
			 System.out.println("Transaction "+t1.transId+" is aborted because of higher timestamp and transaction"
					 +in.transId+" has acquired write lock for data item"+dataItem);
			 releaseLock(t1,dataItem);
		 }
		 else{
			 in.state = "Block";
			 in.waitingOperation.add(new Operation("Write",dataItem));
			 transactionTableMap.put(in.transId,in);						 
			 lock.waitingList.add(in.transId);	
			 System.out.println("Transaction "+in.transId+" has been blocked because of high timestamp and write "
			 		+ "operation for "+ dataItem +" has been added to the waiting operation queue of transaction table and the transactio ID"
				 		+in.transId+" has been added to the waiting list queue of lock table."+"\n");
		 }
		 if(!in.DataItems.contains(dataItem)){
			 in.DataItems.add(dataItem);
			 transactionTableMap.put(in.transId, in);
		 }
			 
		 return lock;
	}
	
	//if the transaction is blocked
	public static void block(String dataItem, Transaction in, String op){
		 if(lockTableMap.containsKey(dataItem)){
			 Lock lock = lockTableMap.get(dataItem);	
			 lock.waitingList.add(in.transId);
			 lockTableMap.put(dataItem,lock); 
		 } 
		 if(!in.DataItems.contains(dataItem))
			 in.DataItems.add(dataItem);
		 in.waitingOperation.add(new Operation(op,dataItem));
		 transactionTableMap.put(in.transId,in); 
		 System.out.println("Transaction "+in.transId+" is in blocked state "+op+" operation on dataitem "
				 +dataItem+" has been added to the waiting operation queue of transaction table and the transactio ID"
				 		+in.transId+" has been added to the waiting list queue of lock table."+"\n");
	}
	
	//when file encounters c
	public static void commit(Transaction in){
		
		if(in.state.equals("Active")){
			System.out.println("Committing Transaction "+in.transId+"\n");
			in.state ="Commit";
			Queue<String>  DataItems = in.DataItems;
			System.out.println("Releasing locks aquired by transaction "+in.transId);
			while(!DataItems.isEmpty()){
				String d = DataItems.remove();
				releaseLock(in, d);
			}
			System.out.println("Transaction "+in.transId+" has been committed and locks have been released\n");
			
			//lock.readTransactionId.remove(in.transId);
			//bring the priority queue element and change the state to of that transaction to active
		}
		else if(in.state.equals("Block")){
			 in.waitingOperation.add(new Operation("Commit", ""));
			 transactionTableMap.put(in.transId, in);	
			 System.out.println("Commit operation on transaction "+in.transId+" has been added to the waiting operation");
		} 
		else if (in.state.equals("Abort")){
			System.out.println("Transaction "+in.transId+" cannot be committed because it has already been aborted.");
		}
	}
	
	//after transaction commits or aborts then this method is called
	public static void releaseLock(Transaction in, String dataItem){
		
		Lock lock = lockTableMap.get(dataItem);
		
		if(lock.lockState.equals("Write") || lock.readTransactionId.size()==1){
			Queue<Integer> wt= lock.waitingList;
			lock.lockState = "";
			if(lock.readTransactionId.size()==1){
				lock.readTransactionId.remove(0);
				System.out.println("Transaction "+in.transId+" has released read lock on "+dataItem);
			}else{
				System.out.println("Transaction "+in.transId+" has released write lock on "+dataItem);
			}
			lockTableMap.put(dataItem, lock);
			if(wt.isEmpty()){
				lockTableMap.remove(dataItem);
				
			} else{
				while(!lock.waitingList.isEmpty()){
					int tid = lock.waitingList.remove();
					Transaction t = transactionTableMap.get(tid);
					
					t = acquireLocks(t, dataItem, lock);
					transactionTableMap.put(tid, t);
					if(!t.state.equals("Commit")){
						return;
					}
				}
			}
			
			lockTableMap.remove(dataItem);	
		}
		else if(lock.lockState.equals("Read")){
			List<Integer> rtids = lock.readTransactionId;
			for(int i = 0; i < rtids.size(); ++i ){
				if(rtids.get(i) == in.transId){
					rtids.remove(i);
				}
			}
			System.out.println("Transaction "+in.transId+" has released read lock on "+dataItem);
			lockTableMap.put(dataItem, lock);
		}
	}
	
	//after transaction holding the lock is committed to give the lock to the waiting transaction this is called
	public static Transaction acquireLocks(Transaction in, String dataItem, Lock lock){
		Queue<Operation> wo = in.waitingOperation;
		in.state="Active";//
		transactionTableMap.put(in.transId,in);//
		
		
		if(!wo.isEmpty()){
			System.out.println("Transaction "+in.transId+" has been changed from Block to Active");
			System.out.println("Running its waiting operations");
		}
		while(!wo.isEmpty()){
			Operation o = wo.remove();
			if(o.operation.equals("Read")){
				request(o.dataItem, in.transId,"r");
			} else if(o.operation.equals("Write")){
				request(o.dataItem, in.transId,"w");
			} else if(o.operation.equals("Commit")){
				commit(in);
				
			}
		
			
		}
		
		lockTableMap.put(dataItem, lock);
		
		return in;
	}
	
	//if the transaction aborts then it releases the locks by calling the release lock function
	public static void abort(Transaction t1){
		 t1.state = "Abort";
		 Queue<String>  DataItems = t1.DataItems;
			System.out.println("Releasing locks aquired by transaction "+t1.transId);
			while(!DataItems.isEmpty()){
				String d = DataItems.remove();
				releaseLock(t1, d);
		 }
		 transactionTableMap.put(t1.transId, t1);
	}
}

