import java.nio.file.Path;
import java.nio.file.Paths;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;

public class Create_Queue 
{
	static Path currentRelativePath = Paths.get("");
	static String current_working_directory = currentRelativePath.toAbsolutePath().toString();			
	static String credentils_file=current_working_directory+"/credentials";
	static AmazonDynamoDBClient connect_dynamoDB;
	private static void initializer() throws Exception  // This method will initialize dynamoDB get_credentials. 
	{       
        AWSCredentials get_credentials = null;
        try 
        {
        	// Get the location of credentials file.
        	ProfilesConfigFile pcf=new ProfilesConfigFile(credentils_file);
        	// read access_key and secrete key from credentials file.
        	get_credentials = new ProfileCredentialsProvider(pcf,"default").getCredentials();  
        } 
        catch (Exception e) 						/// Exception handling.
        {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your get_credentials file is at the correct location" ,
                    e);
        }
        connect_dynamoDB = new AmazonDynamoDBClient(get_credentials);	/// Connect to dynamoDB.
        Region us_West2 = Region.getRegion(Regions.US_WEST_2);
        connect_dynamoDB.setRegion(us_West2);
        execute_DynamoDB();
    }
	public static void execute_DynamoDB() throws Exception    /// This method will create table Job_Id_Check in dynamoDB. 
	{
		//initializer();
		System.out.println("Creating Table !!!");
        try 
        {
            String tableName = "Job_Id_Check";

            // Check if table exist if not create it.
            if (Tables.doesTableExist(connect_dynamoDB, tableName)) 
            {
                System.out.println("Table " + tableName + " is already ACTIVE");
            } 
            else 
            {
                // Create a table 
                CreateTableRequest create_Table = new CreateTableRequest().withTableName(tableName)
                    .withKeySchema(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.HASH))
                    .withAttributeDefinitions(new AttributeDefinition().withAttributeName("Id").withAttributeType(ScalarAttributeType.S))
                    .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(10000L).withWriteCapacityUnits(10000L));
                    TableDescription table_Description = connect_dynamoDB.createTable(create_Table).getTableDescription();
                System.out.println("Created Table: " + table_Description);

                // Wait for it to become active
                System.out.println("Waiting for " + tableName + " to become Live...");                
            }            
            // Describe table.
            //DescribeTableRequest describe_Table = new DescribeTableRequest().withTableName(tableName);
            //TableDescription table_Description = connect_dynamoDB.describeTable(describe_Table).getTable();
            //System.out.println("Table Description is: " + table_Description);
            System.out.println("Table Created !!!");
        } 
        catch (AmazonServiceException ase) 		// Exception handling.
        {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to AWS, but was rejected with an error response for some reason.");            
        } 
        catch (AmazonClientException ace) 
        {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with AWS");
            System.out.println("Error Message: " + ace.getMessage());
        }
        }
	public static void main(String args[]) throws Exception
	{
		//String queue_name=args[0];
		String queue_name=args[0];
		Path currentRelativePath = Paths.get("");
		String current_working_directory = currentRelativePath.toAbsolutePath().toString();			
		String credentils_file=current_working_directory+"/credentials";
		AWSCredentials get_credentials = null;
		String message_Receipt_Handle;
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
        System.out.println("Connecting");
        AmazonSQS connect_Sqs = new AmazonSQSClient(get_credentials);  // Connect to sqs.
        Region us_West2 = Region.getRegion(Regions.US_WEST_2);
        connect_Sqs.setRegion(us_West2);        
        initializer();
        
        System.out.println("Creating "+args[0]);
        CreateQueueRequest queue_Request = new CreateQueueRequest(queue_name);
        String incoming_Queue = connect_Sqs.createQueue(queue_Request).getQueueUrl();
        System.out.println(queue_name+" Url is:- "+incoming_Queue);
        
        // Create response queue.
        System.out.println("\nCreating Response Queue.");
        CreateQueueRequest response_Queue_Request = new CreateQueueRequest("Response_Queue");
        String response_Queue_Url = connect_Sqs.createQueue(response_Queue_Request).getQueueUrl();
        System.out.println("Response_Queue Url is:- "+response_Queue_Url);  
        
        System.out.println("DynamoDB table and Queue successfully created !!!");
	}
}
