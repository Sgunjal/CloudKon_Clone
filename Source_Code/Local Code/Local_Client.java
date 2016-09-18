import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Local_Client implements Runnable   
{ 
    BlockingQueue<String> incoming_queue;   
    String rline=null;								// Variable declaration.
    static long id=0;
    static String file_name=null;
    static long start_Time=0;
    static long end_Time=0;   
    public Local_Client(BlockingQueue<String> q)  // Parameterized constructor declaration.
    {
        this.incoming_queue=q;					// get incoming queue. 
    }
    // This method will insert job in incoming queue.
    public void input_jobs_in_queue()	
    {
    	try 
    	{		
    		FileReader fr=new FileReader(file_name);		
    		BufferedReader br=new BufferedReader(fr);
    		while((rline=br.readLine())!=null)		// Read job from file and put it in queue.
    		{									                     
               // Thread.sleep(i);
            	id++;
    			//incoming_queue.add(id+" "+rline);            	
            	incoming_queue.put(id+" "+rline);		// Put it in queue.
               // System.out.println("Produced "+id+" "+rline);                    
    		}
		br.close();									// Close file.
        //adding exit message              
            incoming_queue.put("Stop");				
        } 
    	catch (InterruptedException e) 		// Exception Handling
    	{
            e.printStackTrace();
        }
		catch (Exception e) 
		{
			e.printStackTrace();
		}
}
    public void run() 				// Run method for thread.
    {
    	System.out.println("Local Client started !!!");
    	input_jobs_in_queue();    	        
    }
    public static void main(String[] args) 	// Main method.
    {  	 
    	try
    	{
		int no_of_threads=Integer.parseInt(args[3]);			// Threads count
		String rline=null;
		file_name=args[5];
        //Creating BlockingQueue of size 10
		// Incoming and response queue declaration.
        BlockingQueue<String> incoming_queue = new ArrayBlockingQueue<>(10000);
        BlockingQueue<String> response_queue = new ArrayBlockingQueue<>(10000);
	start_Time   = System.currentTimeMillis();		// Start timer.
        Local_Client l_client = new Local_Client(incoming_queue);
        Local_Worker l_worker = new Local_Worker(incoming_queue,response_queue,no_of_threads);
        //starting l_client to produce messages in incoming_queue
        
        
        new Thread(l_client).start();
        //Local_Worker consume=new Local_Worker(incoming_queue);
        l_worker.create_worker_threads(l_worker);   
        
        // Check if any other job left to execute.
        while(response_queue.remainingCapacity()!=0)
        {     
        }
        
        int cnt=0;
        cnt=response_queue.size();
        int j=0,success=0,failure=0;
        
        // Check if there are any failures
        while(j<cnt)
        {
        	 j++;
        	 rline=response_queue.take();
        	 if((rline.substring(rline.indexOf(" ")+1)=="1"))        	 
        		 failure++;        	 // Failure counter
        	 else
        		 success++;			// Success counter 
		//System.out.println("J:- "+j+"cnt:- "+cnt);
        }
        end_Time = System.currentTimeMillis();		// Stop timer.      
        System.out.println("Records Failed to process:- "+failure);
        System.out.println("Records Successfully processed:- "+success);
        if(success==j)    
        	System.out.println("All Records Processed Successfully !!!");
        
        long total_Time = end_Time - start_Time;			// Count execution time.			
		System.out.println("Start time is:- "+start_Time);			
		System.out.println("Total Time taken for execution (In Milliseconds):- "+total_Time);  
        
    	}
    	// Exception handling.
    	catch(Exception e)
    	{    		
    	}
    } 
}