import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class splittest {

	public static void main(String [] args)
	{	
		try{
	String AExacttime ="12:30";
	String BExactime = "1:34";
		
		DateFormat df = new SimpleDateFormat("HH:mm");
  		 Date adate = df.parse(AExacttime) ;
  		 Date bdate = df.parse(BExactime);
  	  
  		long diff = adate.getTime() - bdate.getTime();
  		long diffHours = -1* (diff / 1000);
  		 System.out.println(diffHours);
  		 if(diffHours > 3600 && diffHours <=18000 ){
 				 System.out.println("ttttttt"); 
	
	}else{
		System.out.println("not working");
	}
		}catch(Exception e){
			e.printStackTrace();
		}
}
}
//String d1 = "12/11/1993:12:34";
//String d2 = "12/11/1993:1:34";
//DateFormat df1 = new SimpleDateFormat("dd/MM/yyyy:HH:ss");
//
//
//try {
//	Date d12 = df1.parse(d1);
//	Date d22 = df1.parse(d2);
//	
//	long i = d12.getTime() - d22.getTime();
//	
//	if(i > 1&& 1<=5){
//		if(d12.getMonth()== d22.getMonth()){
//		
//		System.out.println("hi");
//		}else
//		{
//			System.out.println("wrong");
//		}
//	}
	
	
//		String 
//		
//		String[] record = input.split(" ");
//		int i =0;
//		for(String s : record){
//			i++;
//			System.out.println("\n"+i+"/" + s);
//		}
//		

		
//	if(input.matches("(.*)associate(.*)"))
//	{
//		
//		String Time = record[1].substring(11, 37);
//		for( String s : record){
//			int i =0; 
//		
//			System.out.println(i +"\t"+ s );
//			i++;
//		}
//		System.out.println("\n loop 1" +Time);
//		
//	
//}else{
//	
//	String[] record = input.split(" - - | - |\"");
//	String Time = record[1].substring(1, 27);
//	for( String s : record){
//		int i =0; 
//	
//		System.out.println(i +"\t"+ s );
//		i++;
//	}
//	System.out.println("\n loop 2 " +Time);
//}
//	}
//}
