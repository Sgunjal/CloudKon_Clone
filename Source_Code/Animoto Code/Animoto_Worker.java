import java.util.*;
import java.util.Map.Entry;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
/*import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;*/
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
/*import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;*/
import com.amazonaws.services.s3.model.PutObjectRequest;

class Animoto_Worker extends Thread 			// Declare Animoto_Worker class
{	
	String thread_name=null,Qname=null;
	static volatile long id=0;
	AmazonS3 connect_s3;						// Variable Declaration section.
	String bucketName;
	String key;
	static String credentils_file=null;
	static AmazonDynamoDBClient connect_dynamoDB;  
	Animoto_Worker()					// default constructor.
	{
	}
	Animoto_Worker(String thread_name,String Qname) // Parametarized Constructor.
	{
		this.thread_name=thread_name;
		this.Qname=Qname;
	}
	 public void create_Bucket()			// Create S3 bucket.
	 {
		 	
		 AWSCredentials get_credentials = null;	
	        try 
	        {
	        	// Get access to profile.
	        	ProfilesConfigFile pcf=new ProfilesConfigFile(credentils_file);
	            get_credentials = new ProfileCredentialsProvider(pcf,"default").getCredentials();  
	        } 
	        catch (Exception e)  // Exception handling. 
	        {
	        		throw new AmazonClientException("Cannot load the credentials from the credential profiles file. " +
	                    "Please make sure that your get_credentials file is at the correct location" ,
	                    e);
	        }	        
	        
	        connect_s3 = new AmazonS3Client(get_credentials); 
	        Region us_West2 = Region.getRegion(Regions.US_WEST_2);
	        connect_s3.setRegion(us_West2);

	        bucketName = "pa3videostorage" + UUID.randomUUID();  // Create Bucket.
	        key = "MyVideo";									// Video key.
	        		 
	     System.out.println("-----Connect Amazon S3-----");
	   	     
	     try 
	     {	           
	            System.out.println("Connecting to bucket " + bucketName + "\n");
	            connect_s3.createBucket(bucketName);	  					// Create Bucket          	            	         
	     }
	 catch(Exception e)		// Exception Handling
	 {
		  e.printStackTrace();
	 }	 
	 }
	synchronized public void run()		// This method is called by every thread.
	{		
		try
		{
			create_Bucket();			// Call create_Bucket method
			execute_jobs();				// Call execute_jobs method
		}			
		catch(Exception e)				// exception Handling.
		{			
			e.printStackTrace();
		}
	}
	private static void initializer() throws Exception 
	{        
        AWSCredentials get_credentials = null;
        try {
        	ProfilesConfigFile pcf=new ProfilesConfigFile(credentils_file);
            get_credentials = new ProfileCredentialsProvider(pcf,"default").getCredentials();  
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your get_credentials file is at the correct location" ,
                    e);
        }
        connect_dynamoDB = new AmazonDynamoDBClient(get_credentials);
        Region us_West2 = Region.getRegion(Regions.US_WEST_2);
        connect_dynamoDB.setRegion(us_West2);
    }
	
	public void execute_jobs() throws Exception
	{
		// Connect All Queues				
		AWSCredentials get_credentials = null;		
		String str_response_Queue_Url=null;
		AmazonSQS connect_Sqs=null;
		int task_counter=0;
		String output_file_name=null;
		int total_jobs=10;
		int No_Of_Files=60;
		int job_counter=0;
        try 
        {
        	// connect 
        	ProfilesConfigFile pcf=new ProfilesConfigFile(credentils_file);
            get_credentials = new ProfileCredentialsProvider(pcf,"default").getCredentials();
        } 
        catch (Exception e)   // exception handling. 
        {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your get_credentials file is at the correct location" ,
                    e);
        }
        try
        {
        String tableName="Animoto_Job_Id";	 // DynamoDb table name.        
        //Map<String, AttributeValue> DynamoDB_item;
        initializer();
        connect_Sqs = new AmazonSQSClient(get_credentials);			//  get credentials to connect SQS
        Region us_West2 = Region.getRegion(Regions.US_WEST_2);
        connect_Sqs.setRegion(us_West2);
               
        System.out.println("-----Connecting Amazon SQS-----\n");        
        
        // Connect incoming Queue.
        System.out.println("Connecting to "+Qname+" Queue");
        GetQueueUrlResult incoming_Queue_Url = connect_Sqs.getQueueUrl(Qname); 
        String str_incoming_Queue_Url =incoming_Queue_Url.getQueueUrl();
        System.out.println("Url is:- "+str_incoming_Queue_Url);
      
        // Connect Response_Queue Queue.
		System.out.println("Connecting to Response_Queue !!");
		GetQueueUrlResult response_Queue_Url = connect_Sqs.getQueueUrl("Animoto_Response_Queue");
	    str_response_Queue_Url =response_Queue_Url.getQueueUrl();
	    System.out.println("Url is:- "+str_response_Queue_Url);
		
	    // Connect DynamoDB
	    System.out.println("Connecting to DynamoDB !!");	    
	    Map<String, AttributeValue> DynamoDB_item;
        PutItemRequest put_Item_Request;
        PutItemResult put_Item_Result;
        //System.out.println("Result: " + put_Item_Result);
               
        GetQueueAttributesRequest requester = new GetQueueAttributesRequest();
        requester = requester.withAttributeNames("ApproximateNumberOfMessages");
        String command=null;
        Process exe_Process=null;
        int last_index=0;
        String file_name=null;
               
        String message_Receipt_Handle=null;
        HashMap<String, Condition> scan_Filter;
        Condition dynamoDB_condition;
        ScanRequest dynamoDB_scan_Request;
        ScanResult dynamoDB_scan_Result;        
        int val=0;       
        requester = requester.withQueueUrl(str_incoming_Queue_Url);
        String message_body=null;                
        // get the approximate number of queue_Messages in the input_Queue                          
        System.out.println("Started executing jobs !!");
        task_counter=0;        
        while(job_counter<total_jobs)     
        {               	
        	job_counter++;
        	output_file_name="Job_"+job_counter+".mpg";
        	task_counter=1;
        	val=0;
        	ReceiveMessageRequest message_Request = new ReceiveMessageRequest(str_incoming_Queue_Url);
    		List<Message> queue_Messages = connect_Sqs.receiveMessage(message_Request).getMessages();
    		for (Message message : queue_Messages) {
    		//	System.out.println("  Message");
    		//	System.out.println("    MessageId:     " + message.getMessageId());
    		//	System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
    		//	System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
    			message_body=message.getBody();
    			//System.out.println("    Body:          " + message_body);
    			for (Entry<String, String> entry : message.getAttributes().entrySet()) {
    			//	System.out.println("  Attribute");
    			//	System.out.println("    Name:  " + entry.getKey());
    			//	System.out.println("    Value: " + entry.getValue());
    			}
    		}      		
            String Id_of_command = message_body.substring(0,message_body.indexOf(" "));
            int id_cmd=Integer.parseInt(Id_of_command);
            String Command_str=message_body.substring(message_body.indexOf(" ")+1);
            //System.out.println("Id is"+id_cmd+"command"+Command_str);
            
            scan_Filter = new HashMap<String, Condition>();
            dynamoDB_condition = new Condition()
                .withComparisonOperator(ComparisonOperator.EQ.toString())
                .withAttributeValueList(new AttributeValue().withN(Id_of_command));
            scan_Filter.put("Id", dynamoDB_condition);
            dynamoDB_scan_Request = new ScanRequest(tableName).withScanFilter(scan_Filter);
            dynamoDB_scan_Result = connect_dynamoDB.scan(dynamoDB_scan_Request);
            
            //System.out.println("DynamoDb check "+ dynamoDB_scan_Result);
            val=dynamoDB_scan_Result.getCount();
            //System.out.println("Records Found "+val);
            
            if(val==0) /// If records does not present insert it dynamodDB  
            {
            	message_Receipt_Handle = queue_Messages.get(0).getReceiptHandle();
                connect_Sqs.deleteMessage(new DeleteMessageRequest(str_incoming_Queue_Url, message_Receipt_Handle));   
                
            	DynamoDB_item = Add_item(id_cmd,Command_str);
                put_Item_Request = new PutItemRequest(tableName, DynamoDB_item);
                put_Item_Result = connect_dynamoDB.putItem(put_Item_Request);
              //  System.out.println("Result: " + put_Item_Result);                
                //connect_Sqs.sendMessage(new SendMessageRequest(str_response_Queue_Url,Command_str ));
               // System.out.println("x"+Command_str+"z");
                FileReader fr=new FileReader(Command_str);  // Open file which has images link.
                BufferedReader br=new BufferedReader(fr);
                                
                while(task_counter<=No_Of_Files)		// Download images from google. 
            	{    
                	Command_str=br.readLine();            	
                	command="wget "+Command_str;
        			exe_Process = Runtime.getRuntime().exec(command);   // Excecute wget command.
        			exe_Process.waitFor();
        		
        			last_index=Command_str.lastIndexOf('/');
        			file_name=Command_str.substring(last_index+1);     // get file name .
        		//	System.out.println("file_name:- "+file_name);        			
        		
        			command="mv "+file_name+" img"+task_counter+".jpg";  // Rename file.
        			exe_Process = Runtime.getRuntime().exec(command);
        			task_counter++;
            	}   
                br.close();
            }               	
        	         
        command="ffmpeg -f image2 -i img%d.jpg "+output_file_name;    // This command will create video file from images.
        //command="ffmpeg -framerate 1/5 -i img%d.jpg -c:v libx264 -r 30 -pix_fmt yuv420p "+output_file_name;
        exe_Process = Runtime.getRuntime().exec(command); 			// Execute command. 
		exe_Process.waitFor();										// Wait for command to finish.
		File f1=null;							
		command="rm *.jpg";											// Remove file. 
		exe_Process = Runtime.getRuntime().exec(command); 	
		exe_Process.waitFor();							
        System.out.println("Uploading "+output_file_name+" to S3 !!!");
        f1=new File(output_file_name);								// Open video file.
        connect_s3.putObject(new PutObjectRequest(bucketName, "Job_"+job_counter+"_Video", f1));  // Upload video file to s3.
        String Video_url=((AmazonS3Client) connect_s3).getResourceUrl(bucketName,"Job_"+job_counter+"_Video");
        System.out.println(Video_url);
        connect_Sqs.sendMessage(new SendMessageRequest(str_response_Queue_Url,Video_url ));   // Get the Video url and put it in response Queue.
        
		System.out.println("Video File is ready  !!");
        }
        }
        catch(IndexOutOfBoundsException iobe)  // Exception handling.
        {        	        
        	String command;
        	Process exe_Process;
        	System.out.println("record_counter "+task_counter);
        	File f;
          /*f=new File("img"+task_counter+".jpg");
            f.delete();*/
            
        	command="ffmpeg -f image2 -i img%d.jpg "+output_file_name;        	
        	//command="ffmpeg -framerate 1/5 -i img%d.jpg -c:v libx264 -r 30 -pix_fmt yuv420p "+output_file_name;
     		exe_Process = Runtime.getRuntime().exec(command); 
     		exe_Process.waitFor();
     		/*f=new File("img"+task_counter+".jpg");
            f.delete();*/
     		command="rm *.jpg";
     		exe_Process = Runtime.getRuntime().exec(command); 	
     		exe_Process.waitFor();
     		//command="rm *.jpg";     				
     		
     		exe_Process.waitFor();
            System.out.println("Uploading "+output_file_name+" to S3 !!!");
            f=new File(output_file_name);
            connect_s3.putObject(new PutObjectRequest(bucketName, "Job_"+job_counter+"_Video", f));            
            String Video_url=((AmazonS3Client) connect_s3).getResourceUrl(bucketName,key);
            System.out.println(Video_url);
            connect_Sqs.sendMessage(new SendMessageRequest(str_response_Queue_Url,Video_url ));
           
    		System.out.println("Video File is ready  !!");
        }
        catch(QueueDoesNotExistException qdne)
        {
        	
        }
	}	
	private static Map<String, AttributeValue> Add_item(int Id, String Command)  // this method will add item to dynamoDB.
	{
        Map<String, AttributeValue> DynamoDB_item = new HashMap<String, AttributeValue>();
        DynamoDB_item.put("Id", new AttributeValue(Integer.toString(Id)));
        DynamoDB_item.put("Command", new AttributeValue(Command));        
        return DynamoDB_item;  
    }
	public static void main(String args[]) throws Exception
	{	
		Thread t[]=new Thread[20];
		int no_of_threads=Integer.parseInt(args[3]);
		Path currentRelativePath = Paths.get("");
		String current_working_directory = currentRelativePath.toAbsolutePath().toString();
		credentils_file=current_working_directory+"/credentials";
					
		//System.out.println("no_of_threads "+no_of_threads+"Qname "+args[2]);
		try
		{			
			for(int i=1;i<=no_of_threads;i++)           // Create threads.
			{
				t[i]=new Animoto_Worker("Thread"+i,args[1]);
				t[i].start(); 
			}
			for (int i=1;i<=no_of_threads;i++) 							 
			{
				t[i].join();											// wait till thread completes its execution.
			}									
		}	
		catch(Exception e)			// Exception handling.
		{
			e.printStackTrace();
		}
	}
}