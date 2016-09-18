import java.util.concurrent.BlockingQueue;
 
public class Local_Worker implements Runnable   // Create local worker.
{
 String s1=null;
 BlockingQueue<String> incoming_queue;		// Queue declaration
 BlockingQueue<String> response_queue;
 private int no_of_threads=0;
     
	public Local_Worker()			// Constructor declaration.
	{	
	}
	// Get incoming_queue,response_queue and no_of_threads as parameter.
    public Local_Worker(BlockingQueue<String> in_q,BlockingQueue<String> res_q,int no_of_threads)
    {
        this.incoming_queue=in_q;
        this.response_queue=res_q;
        this.no_of_threads=no_of_threads;
    }  
    // This method will create worker threads.
    public void create_worker_threads(Local_Worker consumer)
    {
    	for(int i=1;i<=no_of_threads;i++)
    	{    		    	
    		new Thread(consumer).start();    		    	
    	}
    }
    // This method will be called by all threads and will execute the jobs.  
    public void run() 
    {
    	String rline=null;
    	System.out.println("Local Worker started !!!");
        try
        {           
            //execute jobs until Stop message is received
            while((rline = incoming_queue.take())!="Stop")
            {
            	s1=rline.substring(rline.indexOf("(")+1,rline.indexOf(")"));				
				//System.out.println(thread_name+" "+s1);
            	// Execute sleep job.
				Thread.sleep(Integer.parseInt(s1));
				response_queue.put(rline.substring(0,rline.indexOf(" "))+" "+0);
				//System.out.println("ID is:-"+rline.substring(0,rline.indexOf(" "))+"x");				
            }
		System.out.println("Worker execution complete !!!");
        }
        catch(InterruptedException e) // Exception handling.
        {   
        	try
        	{
        		// If failure insert 1 in response queue.
        		response_queue.put(rline.substring(0,rline.indexOf(" "))+" "+1);
        	}
        	catch(InterruptedException e1)  // Exception handling.
        	{
        		e1.printStackTrace();
        	}        
            e.printStackTrace();
        }
    }
}