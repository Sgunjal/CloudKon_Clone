import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;

class Client extends Thread
{	
	String thread_name=null;
	static long id=0;
	static String workload_file=null;
	static String queue_name=null;
	static String credentils_file=null;
	static long start_Time=0;
	static long end_Time=0;
	String message_body=null;
	static AmazonDynamoDBClient connect_dynamoDB;
	Client()								// default constructor.
	{
	}
	Client(String thread_name)				// Parameterized constructor.
	{
		this.thread_name=thread_name;	
	}
	public synchronized void insert_in_SQS() // This method will insert jobs in SQS queue.
	{
		AWSCredentials get_credentials = null;		
        try 
        {
        	// Get credentials.
        	ProfilesConfigFile pcf=new ProfilesConfigFile(credentils_file);
        	// connect to queue.
            get_credentials = new ProfileCredentialsProvider(pcf,"default").getCredentials();
        } 
        catch (Exception e) 		// Exception hadling.
        {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your get_credentials file is at the correct location" ,
                    e);
        }
        AmazonSQS connect_Sqs = new AmazonSQSClient(get_credentials);  // Connect to sqs.
        Region us_West2 = Region.getRegion(Regions.US_WEST_2);
        connect_Sqs.setRegion(us_West2);
        
        System.out.println("-----Getting Started with Amazon SQS-----\n");

        try 
        {
            // Create a incoming queue
            System.out.println("Connecting Incoming Queue.");
          // CreateQueueRequest queue_Request = new CreateQueueRequest(queue_name);
            
            GetQueueUrlResult incoming_Queue_Result = connect_Sqs.getQueueUrl(queue_name);
            String incoming_Queue_Url =incoming_Queue_Result.getQueueUrl();                                            
            System.out.println(queue_name+" Url is:- "+incoming_Queue_Url);
            
            // Create response queue.
            System.out.println("\nConnecting Response Queue.");
            GetQueueUrlResult response_Queue_Result = connect_Sqs.getQueueUrl("Response_Queue");
            String response_Queue_Url =response_Queue_Result.getQueueUrl();                                                      
            System.out.println("Response_Queue Url is:- "+response_Queue_Url); 
            
            // Read workload file.
            FileReader fr=new FileReader(workload_file);
			BufferedReader br=new BufferedReader(fr);		
			//System.out.println("Enter the number of threads to create:- ");			
			String rline=null;
		
			System.out.println("Sending messages to "+queue_name+"\n");            
			start_Time   = System.currentTimeMillis();		// Start timer.
			while((rline=br.readLine())!=null)
			{
				id++;
				connect_Sqs.sendMessage(new SendMessageRequest(incoming_Queue_Url, id+" "+rline));	
				//System.out.println(rline);
			}	
			//connect_Sqs.sendMessage(new SendMessageRequest(incoming_Queue, 9999999+" "+"Exit(0)"));
			br.close();
			
			// Reading number of messages in queue.
			GetQueueAttributesRequest requester = new GetQueueAttributesRequest();
	        requester = requester.withAttributeNames("ApproximateNumberOfMessages");
	        
	        //int max = 25;
   
	        requester = requester.withQueueUrl(response_Queue_Url);

	        Map<String, String> attrs_Map = connect_Sqs.getQueueAttributes(requester).getAttributes();

	        // get the approximate number of messages in the queue
	        int messages_in_queue = Integer.parseInt(attrs_Map.get("ApproximateNumberOfMessages"));
	        //System.out.println("Queue Size is:- "+messages_in_queue);    
			System.out.println("Checking if execution is completed !!!!");
	        while((messages_in_queue)<id)					// while all the messages finished execution.
	        {
	            sleep(10);
	            requester = new GetQueueAttributesRequest();
		        requester = requester.withAttributeNames("ApproximateNumberOfMessages");
		        
		        //int max = 25;	   
		        requester = requester.withQueueUrl(response_Queue_Url);
		        attrs_Map = connect_Sqs.getQueueAttributes(requester).getAttributes();
		        // get the number of messages in the queue.
		        
		        messages_in_queue = Integer.parseInt(attrs_Map.get("ApproximateNumberOfMessages"));		        
		        //System.out.println(messages_in_queue+"Checking Response Queue !!!!");
	        }  
	        end_Time   = System.currentTimeMillis();		// Stop timer.
	        if((messages_in_queue)==id)			// Check if incoming messages and outgoing messages count.
	        {
	        	System.out.println("All Messages Processed !!!");
	        }
	        else						
	        {
	        	//System.out.println((messages_in_queue)+"Few Messages Not Processed !!!");
	        }	        
           // System.out.println("Deleting both queues.\n");
            //connect_Sqs.deleteQueue(new DeleteQueueRequest(incoming_Queue));
            //connect_Sqs.deleteQueue(new DeleteQueueRequest(response_Queue_Url));            
            
            System.out.println("Execution Complete !!!");            
        	}
        
        catch(Exception e)					// Exception handling.
        {
        		e.printStackTrace();
        }
        }
	 
	public static void main(String args[]) throws Exception   	// main method.
	{			
		try
		{	
			queue_name=args[1];							// Queue Name.
			
			Path currentRelativePath = Paths.get("");
			String current_working_directory = currentRelativePath.toAbsolutePath().toString();			
			
			workload_file =current_working_directory+"/"+args[3];		// Workload file location.
			credentils_file=current_working_directory+"/credentials";
						
			Client lc=new Client();
			//lc.execute_DynamoDB();
			lc.insert_in_SQS();			
			long total_Time = end_Time - start_Time;			// Count execution time.			
			System.out.println("Start time is:- "+start_Time);			
			System.out.println("Total Time taken for execution (In Milliseconds)"+total_Time);  		
		}	
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}