import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;


public class Main {

	public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
		//Open file
		//Import data from file
		//Process data into data structure
		
		//JSON Data structure, uses Server instance as key, and data network/time/value in wrapper function as value
		Map<String, Wrapper> JSONData = new HashMap<>(); 
		
		//Creates JSON Array structure to iterate over JSON
		JSONParser jsonParser = new JSONParser();
		File file = new File("data.json");
		Object obj = jsonParser.parse(new FileReader(file));
		JSONArray jsonArray = (JSONArray) obj;
		
		//String values to retrieve values from JSON
		String ip = "instance";
		String network = "data_network";
		String timestamp = "timestamp";
		String value = "value";
		
		//Iterates over JSON and adds all values to HashMap structure
		for(int index = 0; index < jsonArray.size(); index++) {
			//System.out.println(jsonArray.get(index));
			Object j = jsonArray.remove(index);
			JSONObject jobj = (JSONObject) j;
			if(jobj.get(value) != null) {
				JSONData.put((String)jobj.get(ip), new Wrapper((String)jobj.get(network), OffsetDateTime.parse((CharSequence) jobj.get(timestamp)) , Integer.parseInt((String) jobj.get(value))));
			}else {
				JSONData.put((String)jobj.get(ip), new Wrapper((String)jobj.get(network), OffsetDateTime.parse((CharSequence) jobj.get(timestamp)) , 0));
			}
			
			
		}
		
		
		
		
		//Calculate the total average number of queries that each server (data1 and data2 combined) received
		
		//Calculate the average number of queries that were received on data1 and data2 for all instances
		
		//Print the number of spikes each instance (both data1 and data2 inclusive) saw. A spike is defined as any value of 700 and up
		
		//Profile your code. Add timestamps between each step 1 to 4 above and print the time(in any unit you want) it took to output the result

	}

}

//Wrapper class for holding JSON Data
//Assumes data has data network stored as string, time set in ISO 8601 format, and value stored as integer
class Wrapper{
	
	private String dataNetwork;
	private OffsetDateTime time;
	private int value;
	
	public Wrapper(String dataNetwork, OffsetDateTime time, int value) {
		this.dataNetwork = dataNetwork;
		this.time = time;
		this.value = value;
	}
	
	public String getDataNetwork(){return this.dataNetwork;}
	public OffsetDateTime getTime(){return this.time;}
	public int getValue(){return this.value;}
	
	
}

/*for(Wrapper w: JSONData.values()) {
System.out.println(w.getDataNetwork());
System.out.println(w.getTime());
System.out.println(w.getValue());

}*/