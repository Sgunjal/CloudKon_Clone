import java.io.*;
import java.nio.*;
import java.nio.file.Path;
import java.nio.file.Paths;
public class Workload_File_Creator 
{
public static void main(String args[]) throws Exception
{
	Path currentRelativePath = Paths.get("");
	String current_working_directory = currentRelativePath.toAbsolutePath().toString();	
	
	DataInputStream dis=new DataInputStream(System.in);
	File f =new File(current_working_directory+"/Workload.txt");
	FileWriter fw=new FileWriter(current_working_directory+"/Workload.txt");
	BufferedWriter bw=new BufferedWriter(fw);
	
	System.out.println("Enter number of sleep jobs to create:- ");
	int sleep_task=Integer.parseInt(dis.readLine());
	System.out.println("Enter the sleep time for job (milliseconds):- ");
	int sleep_time=Integer.parseInt(dis.readLine());
	
	for (int i=1;i<=sleep_task;i++)
	{
		bw.write("Thread."+"sleep("+sleep_time+");");
		bw.newLine();
	}
	bw.close();	
}

}
