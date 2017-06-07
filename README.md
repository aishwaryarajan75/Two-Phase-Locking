Data Structures:
The data structures that we are going to use for implementing Rigorous 2PL are as follows:
Hash Map: to populate and update  the transaction table and lock table
Priority Queue: To maintain transaction waiting for write or read for a data item.
List: to maintain readtransactionId in the lock table and the dataItems in the transaction table (since read can be accessed by many transactions in the lock table and one transaction can have many dataItems associated with it in the transactions table)


Pseudo Code:
Firstly we will read the file and while reading the file we are using switch statement and calling the methods for the b,r,w and e operation 
1.	‘b’ is encountered, this will call the begin transaction function and will make entry in the transaction table for the following values:
trans.id;              //populate transaction id in the hashmap for transaction table
timestamp         //populate the timestamp
state="active"  //mark the state as active
		 				 
2.	‘r’ is encountered, read function is called this will make changes in the lock table and add value of data item in the transaction table and update the transaction table if the state of any of the transaction has changed. 

Before we read any transaction first we need to check its state in the transaction table 
•	If the state is active then we will
check if this dataItem is present in the lock table or not.
 If the Item is present 
then check the state if  state == “read”
append the transactionId in the lock table
				if state==”write”
then check the timestamp of the transactions and apply the wound-wait concept
		if the datatItem is not present then we will make the entry in the table
•	If the state is blocked 
check if this dataItem is present in the lock table or not.
	If present then add the transactionId in the waiting list
If not present then make the entry in the lock table
	Change state to active in the transaction table and update the lock table

•	If the state is aborted then do nothing 
•	If the state is committed then do nothing


3.	‘w’ is encountered , this will make changes in the lock table and add data item in the transaction table and update the transaction table if the state of any of the transaction has changed. The lock table will be update like this:
Before we write any transaction we need to check the status of the transaction 
•	If the state is active then we will
check if this dataItem is present in the lock table or not.
 If the Item is present 
then check the state if  state == “read”
If only one transaction present 
upgrade the lock to write
					else 
check the timestamp of the transactions and apply wound-wait concept
				if state==”write”
then check the timestamp of the transactions and apply the wound-wait concept
		if the datatItem is not present then we will make the entry in the table

•	If the state is blocked 
check if this dataItem is present in the lock table or not.
	If present then add the transactionId in the waiting list
If not present then make the entry in the lock table
	Change state to active in the transaction table and update the lock table

•	If the state is aborted then do nothing 
•	If the state is committed then do nothing

4.	‘e’ is encountered this will make changes in the lock table and will update the transaction table accordingly
Before we end any transaction we need to check the status of the transaction 
•	If the state is active then we will
Release all the locks from the transactions table and change state of the transaction in the transaction table to commit. Bring the next transaction from the priority queue that has been waiting.				

•	If the state is blocked 
We will check the lock state of the dataItems for which the transaction was locked. If the locks are released we will apply the locks after all the locks are provided later we will change the change the state to commit in the transactions table.

