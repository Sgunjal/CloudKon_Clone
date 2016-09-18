import java.nio.file.Path;
import java.io.*;
import java.nio.file.Paths;

public class Input_File_Creator
{
	public static void main(String args[]) throws Exception
	{
	int i=1;
	File f =new File("Animoto_Workload.txt");
	FileWriter fw=new FileWriter("Animoto_Workload.txt");
	BufferedWriter br =new BufferedWriter(fw);
	Path currentRelativePath = Paths.get("");
	String current_working_directory = currentRelativePath.toAbsolutePath().toString();	
	while(i<160)
	{
		i++;
		br.write(current_working_directory+"/Image_Link.txt");
		br.newLine();
	}
	br.close();
	}
}