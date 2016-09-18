import java.util.*;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.amazonaws.auth.profile.*;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
/*import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;*/
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;   
import com.amazonaws.services.sqs.model.SendMessageRequest;

class Animoto_Client extends Thread     
{
	String thread_name=null;                                                      
	static String file_name=null;
	static long id=0;
	int No_of_jobs=11;
	static String credentils_file=null;
	static AmazonDynamoDBClient dynamoDB_client;
	static String Qname=null;
	static long start_Time=0;
	static long end_Time=0;
	Animoto_Client()
	{
	}
	Animoto_Client(String thread_name)
	{
		this.thread_name=thread_name;	
	}
	private static void initializer() throws Exception   // Get the get_credentials  
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
                    "Please check that your get_credentials file is at the correct location" ,
                    e);
        }
        dynamoDB_client = new AmazonDynamoDBClient(get_credentials);
        Region US_WEST2 = Region.getRegion(Regions.US_WEST_2);
        dynamoDB_client.setRegion(US_WEST2);
    }
	public void execute_DynamoDB() throws Exception
	{
		initializer();
		System.out.println("Creating Table !!!");
        try {
            String table_Name = "Animoto_Job_Id";
            
            					// Create table if it does not exist
            if (Tables.doesTableExist(dynamoDB_client, table_Name)) 
            {
                System.out.println("Table " + table_Name + " is already Present");
            } 
            else 
            {          
            	// Create table request
                CreateTableRequest table_Creator = new CreateTableRequest().withTableName(table_Name) // Create table 
                    .withKeySchema(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.HASH))
                    .withAttributeDefinitions(new AttributeDefinition().withAttributeName("Id").withAttributeType(ScalarAttributeType.S))
                    .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
                    TableDescription Table_Description = dynamoDB_client.createTable(table_Creator).getTableDescription();
                System.out.println("Created Table: " + Table_Description);
                
                System.out.println("Waiting for " + table_Name + " to become Live !!!");
                Tables.awaitTableToBecomeActive(dynamoDB_client, table_Name);
            } 
            	/// New table description.
            DescribeTableRequest describe_Table_Request = new DescribeTableRequest().withTableName(table_Name);
            TableDescription table_Description = dynamoDB_client.describeTable(describe_Table_Request).getTable();
            System.out.println("Table Description:- " + table_Description);
            System.out.println("Table Created !!!");
        } 
        catch (AmazonServiceException ase)           // exception handling
        {
            System.out.println("Your request To create table rejected !!!");           
        } 
        catch (AmazonClientException ace) 	 // exception handling
        {
            System.out.println("Caught an AmazonClientException, means the client encountered "
                    + "a serious internal problem while trying to communicate with AWS");
            System.out.println("Error Message: " + ace.getMessage());
        }
        }
		public void insert_in_SQS()     // Insert jobs in SQS.
		{
			AWSCredentials get_credentials = null; 		// Create AWSCredentials object.
			try 
			{
				// Create ProfilesConfigFile object and get credentials.
				ProfilesConfigFile pcf=new ProfilesConfigFile(credentils_file);
	            get_credentials = new ProfileCredentialsProvider(pcf,"default").getCredentials();  
			} 
			catch (Exception e) 			/// Exception handling.
			{
				throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your get_credentials file is at the correct location" ,
                    e);
			}
			// Create AmazonSQS object to connect to SQS 	
			AmazonSQS connect_Sqs = new AmazonSQSClient(get_credentials);  
			Region US_WEST2 = Region.getRegion(Regions.US_WEST_2);
			connect_Sqs.setRegion(US_WEST2);
			
			System.out.println("-----Connecting to Amazon SQS-----\n");
        try 
        {
            // Create a Workload Queue.
            System.out.println("Creating "+ Qname +".\n");
            CreateQueueRequest queue_Request = new CreateQueueRequest(Qname);
            String Input_Queue_Url = connect_Sqs.createQueue(queue_Request).getQueueUrl();
            System.out.println("Url is:- "+Input_Queue_Url);
            
            // Create Response Queue.
            System.out.println("Creating Animoto_Response_Queue.\n");
            CreateQueueRequest response_Queue_Request = new CreateQueueRequest("Animoto_Response_Queue");
            String response_Queue_Url = connect_Sqs.createQueue(response_Queue_Request).getQueueUrl();
            System.out.println("Url is:- "+response_Queue_Url);
            
            //FileReader fr=new FileReader("/home/sujay/workspace/S3_Samples/src/Workload.txt");
			//BufferedReader br=new BufferedReader(fr);			
			//System.out.println("Enter the number of threads to create:- ");						
		
			System.out.println("Sending a messages to "+Qname+".\n");            
            id=1;
            start_Time   = System.currentTimeMillis();		// Start timer.
            // Read Workload file and put jobs in SQS Queue.
            FileReader fr=new FileReader(file_name);
            BufferedReader br=new BufferedReader(fr);  
			while(id<No_of_jobs)						// Read All Jobs.
			{					
				// Put messages in Queue.
				connect_Sqs.sendMessage(new SendMessageRequest(Input_Queue_Url, id+" "+br.readLine()));	
				id++;
			}	
			br.close() ;
			id=id-1;
			
			// Check Response queue for completion of Jobs.
			GetQueueAttributesRequest request1 = new GetQueueAttributesRequest();
	        request1 = request1.withAttributeNames("ApproximateNumberOfMessages");
	        	      
	        request1 = request1.withQueueUrl(response_Queue_Url);	 
	        Map<String, String> attrs = connect_Sqs.getQueueAttributes(request1).getAttributes();
	        int messages_in_queue = Integer.parseInt(attrs.get("ApproximateNumberOfMessages"));

	        // get the approximate number of messages in the queue	        
	        //System.out.println("Queue Size is:- "+messages_in_queue);    
			System.out.println("Checking if execution complete !!!!");
	        while(messages_in_queue<id)
	        {
	            sleep(500);
	            request1 = new GetQueueAttributesRequest();
		        request1 = request1.withAttributeNames("ApproximateNumberOfMessages");
		        
		        //int max = 25;
	   
		        request1 = request1.withQueueUrl(response_Queue_Url);

		        attrs = connect_Sqs.getQueueAttributes(request1).getAttributes();

		        // get the approximate number of messages in the queue
		        messages_in_queue = Integer.parseInt(attrs.get("ApproximateNumberOfMessages"));
		        //System.out.println("Queue Size is:- "+messages_in_queue);		       
	        }   
	        
	        if(messages_in_queue==id)
	        {
	        	System.out.println("All Messages Processed !!!");
	        }
	        else
	        {
	        	//System.out.println("Few Messages Not Processed !!!");
	        }
           // System.out.println("Deleting both queues.\n");
			end_Time   = System.currentTimeMillis();		// Stop timer.
           // connect_Sqs.deleteQueue(new DeleteQueueRequest(Input_Queue_Url));
            //connect_Sqs.deleteQueue(new DeleteQueueRequest(response_Queue_Url));
			
            System.out.println("Execution Complete !!!");
        	}
        	catch(Exception e)
        	{
        		e.printStackTrace();
        	}
        }		 
	public static void main(String args[]) throws Exception
	{	
		try
		{	
			Qname=args[1];
			
			// Start Timer
			//long start_Time = System.currentTimeMillis();
			Path currentRelativePath = Paths.get("");
			String current_working_directory = currentRelativePath.toAbsolutePath().toString();			
			file_name = current_working_directory+"/"+args[3];
						
			credentils_file=current_working_directory+"/credentials";
			
			Animoto_Client lc=new Animoto_Client();			
			lc.execute_DynamoDB();			// Call execute_DynamoDB Method.
			lc.insert_in_SQS();				// Call insert_in_SQS method.
			
			
			// End Timer			
			long total_Time = end_Time - start_Time;
			
			// Print time taken for execution.
			System.out.println("Start time is:- "+start_Time);			
			System.out.println("Total Time taken for execution (In Milliseconds)"+total_Time);			
		}	
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}