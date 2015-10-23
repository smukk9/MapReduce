import java.io.IOException;
import java.util.StringTokenizer;


public class test {

	public static void main (String [] args) throws IOException{
		
		
		String line = value.toString();
	  
	    int joinIndex;
	   
	    StringTokenizer st = new StringTokenizer(line, ",");
	    int index = 0;
	    String outKey = "", outTuple = "";
	    while (st.hasMoreTokens()) {
	      String r = st.nextToken();
	      index++;
	      if (index==joinIndex)
	        outKey += r;
	      else
	        outTuple += r + ",";
	    }
	    // removing the extra "," at the end
	    outTuple = outTuple.substring(0, outTuple.length() - 1);
	    
	    
	  }
	}

	}
	}
}
