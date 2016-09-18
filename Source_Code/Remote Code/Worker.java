import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
class Worker extends Thread									// Worker class declaration.
{	
	String thread_name=null,Qname=null;
	static long id=0;
	static String credentils_file=null;
	static AmazonDynamoDBClient connect_DynamoDB;
	Worker()
	{
	}
	Worker(String thread_name,String Qname)						// Parameterized Constructor.
	{
		this.thread_name=thread_name;
		this.Qname=Qname;
	}
	private synchronized static void initializer() throws Exception  		// get get_credentials. 
	{    
        AWSCredentials get_credentials = null;
        try 
        {
        	ProfilesConfigFile pcf=new ProfilesConfigFile(credentils_file);
            get_credentials = new ProfileCredentialsProvider(pcf,"default").getCredentials();  
        } 
        catch (Exception e) 
        {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct location",
                    e);
        }
        connect_DynamoDB = new AmazonDynamoDBClient(get_credentials);
        Region us_West2 = Region.getRegion(Regions.US_WEST_2);
        connect_DynamoDB.setRegion(us_West2);
    }
	synchronized public void run()											// This method is called by every thread.
	{		
		try
		{			
			execute_jobs();
		}			
		catch(Exception e)
		{			
			e.printStackTrace();
		}
	}	
	public synchronized void  execute_jobs() throws Exception			// This method will put message in SQS queue and execute them	
	{
		// Connect All Queues				
		AWSCredentials get_credentials = null;
        try 
        {
        	// Get the AWS credentials.
        	ProfilesConfigFile pcf=new ProfilesConfigFile(credentils_file);
            get_credentials = new ProfileCredentialsProvider(pcf,"default").getCredentials();  
        } 
        catch (Exception e) 		// Exception Handling.
        {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct location" ,
                    e);
        }
        try
        {
        String table_Name="Job_Id_Check";			// DynamoDB table name.
        
        //Map<String, AttributeValue> dynamoDB_Item;

        AmazonSQS connect_Sqs = new AmazonSQSClient(get_credentials);	
        Region us_West2 = Region.getRegion(Regions.US_WEST_2);
        connect_Sqs.setRegion(us_West2);
        
        // Connect to incoming queue.
        //System.out.println("Connecting Amazon SQS.\n");  	              
        System.out.println(thread_name+" Connecting to "+Qname+" Queue");
        GetQueueUrlResult incoming_Queue_Result = connect_Sqs.getQueueUrl(Qname);
        String incoming_Queue_Url =incoming_Queue_Result.getQueueUrl();
       // System.out.println(Qname+" Url is:- "+incoming_Queue_Url);
      
        //Connect to Response_Queue.
		System.out.println(thread_name+" Connecting to Response_Queue !!");
		GetQueueUrlResult response_Queue_Result = connect_Sqs.getQueueUrl("Response_Queue");
	    String response_Queue_Url =response_Queue_Result.getQueueUrl();
	    //System.out.println("Response_Queue Url is:- "+response_Queue_Url);
		
	    //Connect to DynamoDB.
	    System.out.println(thread_name+" Connecting to DynamoDB !!");
	    initializer();
	    Map<String, AttributeValue> dynamoDB_Item;
        PutItemRequest dynamoDB_Put_Item_Request;
        PutItemResult dynamoDB_Put_Item_Result;
        //System.out.println("Result: " + dynamoDB_Put_Item_Result);
        GetQueueAttributesRequest requester = new GetQueueAttributesRequest();
        requester = requester.withAttributeNames("ApproximateNumberOfMessages");
        
        //int max = 25;
        HashMap<String, Condition> dynamoDB_Scan_Filter;
        Condition dynamoDB_Condition;
        ScanRequest dynamoDB_Scan_Request;
        ScanResult dynamoDB_Scan_Result;
        String message_Receipt_Handle;
        ReceiveMessageRequest message_Request;
        List<Message> messages_Text;
        int id_cmd;
        String Id_of_command;
        String Command_str;
        int val;
        String s1=null;

        requester = requester.withQueueUrl(incoming_Queue_Url);
        String message_body=null;
        Map<String, String> attrs_Map = connect_Sqs.getQueueAttributes(requester).getAttributes();
        
        // get the approximate number of messages_Text in the queue
        int messages_in_queue = Integer.parseInt(attrs_Map.get("ApproximateNumberOfMessages"));
        System.out.println(thread_name +" Started Processing Records !!!");
        while(true)					// execute while there are messages in queue.
        {
        	while((messages_in_queue)==0)
        	{
        		Thread.sleep(10);
        		requester = new GetQueueAttributesRequest();
                requester = requester.withAttributeNames("ApproximateNumberOfMessages");
                requester = requester.withQueueUrl(incoming_Queue_Url);            
                attrs_Map = connect_Sqs.getQueueAttributes(requester).getAttributes();            
                // get the approximate number of messages_Text in the queue
                messages_in_queue = Integer.parseInt(attrs_Map.get("ApproximateNumberOfMessages"));
        	}
        	if((messages_in_queue)>0)
        	{
        		
        	message_Request = new ReceiveMessageRequest(incoming_Queue_Url);
            messages_Text = connect_Sqs.receiveMessage(message_Request).getMessages();
            for (Message message : messages_Text) {
               // System.out.println("  Message");
                //System.out.println("    MessageId:     " + message.getMessageId());
                //System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
                //System.osleep(100);ut.println("    MD5OfBody:     " + message.getMD5OfBody());
                message_body=message.getBody();
              //  System.out.println("    Body:          " + message_body);
               // for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                  //  System.out.println("  Attribute");
                   // System.out.println("    Name:  " + entry.getKey());
                   // System.out.println("    Value: " + entry.getValue());
               // }
            }                                   
            // Separate Id and message.
            Id_of_command = message_body.substring(0,message_body.indexOf(" "));
            id_cmd=Integer.parseInt(Id_of_command);
            Command_str=message_body.substring(message_body.indexOf(" "));
            //System.out.println("Id is"+id_cmd+"command"+Command_str);
                        
            // Check in dynamoDB is Id already present.
            dynamoDB_Scan_Filter = new HashMap<String, Condition>();
            dynamoDB_Condition = new Condition()
                .withComparisonOperator(ComparisonOperator.EQ.toString())
                .withAttributeValueList(new AttributeValue().withN(Id_of_command));
            dynamoDB_Scan_Filter.put("Id", dynamoDB_Condition);
            dynamoDB_Scan_Request = new ScanRequest(table_Name).withScanFilter(dynamoDB_Scan_Filter);
            dynamoDB_Scan_Result = connect_DynamoDB.scan(dynamoDB_Scan_Request);
                        
           // System.out.println("DynamoDb check "+ dynamoDB_Scan_Result);
            val=dynamoDB_Scan_Result.getCount();
           // System.out.println("Records Found "+val);
            
            // If Id already present in DynamoDB then dont execute that command else insert the id in dynamoDB and execute them.             
            if(val==0)
            {               
            	s1=Command_str.substring(Command_str.indexOf("(")+1,Command_str.indexOf(")"));				
				//System.out.println(thread_name+" "+s1);	            	
            	message_Receipt_Handle = messages_Text.get(0).getReceiptHandle();
            	connect_Sqs.deleteMessage(new DeleteMessageRequest(incoming_Queue_Url, message_Receipt_Handle));
            	
            	dynamoDB_Item = insert_Item(id_cmd,Command_str);
                dynamoDB_Put_Item_Request = new PutItemRequest(table_Name, dynamoDB_Item);
                dynamoDB_Put_Item_Result = connect_DynamoDB.putItem(dynamoDB_Put_Item_Request);
                Thread.sleep(Integer.parseInt(s1));
                connect_Sqs.sendMessage(new SendMessageRequest(response_Queue_Url,Id_of_command +" 0" ));
                // System.out.println("Result: " + dynamoDB_Put_Item_Result);                                                                           	                           	
            }            
            // Get number of messages in queue.
            requester = new GetQueueAttributesRequest();
            requester = requester.withAttributeNames("ApproximateNumberOfMessages");
            requester = requester.withQueueUrl(incoming_Queue_Url);            
            attrs_Map = connect_Sqs.getQueueAttributes(requester).getAttributes();            
            // get the approximate number of messages_Text in the queue
            messages_in_queue = Integer.parseInt(attrs_Map.get("ApproximateNumberOfMessages"));
           // System.out.println(messages_in_queue);
        	}
        	}        	     
        }
        catch(IndexOutOfBoundsException iobe)			// Exception Handling.
        {
        	//iobe.printStackTrace();
        	//System.out.println("IndexOutOfBoundsException");
        }
        catch(QueueDoesNotExistException qdne)			// Exception Handling.
        {        
        }
	}	
	private synchronized static Map<String, AttributeValue> insert_Item(int Id, String Command)  // Insert record in dynamoDB.
	{
        Map<String, AttributeValue> dynamoDB_Item = new HashMap<String, AttributeValue>();
        dynamoDB_Item.put("Id", new AttributeValue(Integer.toString(Id)));
        dynamoDB_Item.put("Command", new AttributeValue(Command));        
        return dynamoDB_Item;
    }
	public static void main(String args[]) throws Exception
	{	
		Thread t[]=new Thread[20];
		int no_of_threads=Integer.parseInt(args[3]);
		//System.out.println("no_of_threads "+no_of_threads+args[2]+"Incoming_Queue");
		Path currentRelativePath = Paths.get("");
		String current_working_directory = currentRelativePath.toAbsolutePath().toString();
		credentils_file=current_working_directory+"/credentials";
		try
		{			
			for(int i=1;i<=no_of_threads;i++)
			{
				t[i]=new Worker("Thread"+i,args[1]);			// Create thread.
				t[i].start(); 
			}
			for (int i=1;i<=no_of_threads;i++) 							// Wait till all files send to slaves.
			{
				t[i].join();											// wait till thread completes its execution.
			}	
			//System.out.println("All Records Processed !!!");
		}	
		catch(Exception e)										// Exception Handling.
		{
			e.printStackTrace();
		}
	}
}