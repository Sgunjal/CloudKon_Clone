import java.nio.file.Path;
import java.nio.file.Paths;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;


public class Delete_Queue 
{
	public static void main(String args[]) throws Exception
	{
		//String queue_name=args[0];
		String queue_name=args[0];
		Path currentRelativePath = Paths.get("");
		String current_working_directory = currentRelativePath.toAbsolutePath().toString();			
		String credentils_file=current_working_directory+"/credentials";
		AWSCredentials get_credentials = null;
		String message_Receipt_Handle;
       System.out.println("Connecting");
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
        
        System.out.println("Deleting "+args[0]);
        GetQueueUrlResult incoming_Queue_Result = connect_Sqs.getQueueUrl(queue_name);
        String incoming_Queue_Url =incoming_Queue_Result.getQueueUrl();        
        connect_Sqs.deleteQueue(new DeleteQueueRequest(incoming_Queue_Url));
        
        System.out.println("Deleting Response_Queue");
        GetQueueUrlResult response_Queue_Result = connect_Sqs.getQueueUrl("Response_Queue");
	    String response_Queue_Url =response_Queue_Result.getQueueUrl();
        connect_Sqs.deleteQueue(new DeleteQueueRequest(response_Queue_Url));           
        
        System.out.println("Queue Deleted Successfully !!!");
	}
}

