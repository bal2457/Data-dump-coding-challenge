import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;


public class Main {

	public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
		
		
		Timestamp startTimestamp = new Timestamp(System.currentTimeMillis());
		
		//1. Ingest attached data from a file
		
		//JSON Data arraylist with instance/network/timestamp/value in wrapper class
		ArrayList<JSONWrapper> JSONData = new ArrayList<JSONWrapper>();
		
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
		
		
		//Iterates over JSON and adds all values to ArrayList structure
		for(int index = 0; index < jsonArray.size(); index++) {
			
			Object j = jsonArray.get(index);
			JSONObject jobj = (JSONObject) j;
			if(jobj.get(value) != null) {
				JSONData.add(new JSONWrapper((String)jobj.get(network), (String)jobj.get(ip), OffsetDateTime.parse((CharSequence) jobj.get(timestamp)) , Integer.parseInt((String) jobj.get(value))));	
			}
			else {
				JSONData.add(new JSONWrapper((String)jobj.get(network), (String)jobj.get(ip), OffsetDateTime.parse((CharSequence) jobj.get(timestamp)) , 0));
			}	
		}

		Timestamp firstTimestamp = new Timestamp(System.currentTimeMillis());
		
		
		
		
		
		
		
		//2. Calculate the total average number of queries that each server (data1 and data2 combined) received
		
		//Data structure that holds server instance as key and wrapper class for queries as value
		HashMap<String, ServerQueryWrapper> averageServerQueryMap = new HashMap<String, ServerQueryWrapper>();
		
		for(JSONWrapper w: JSONData){
			String serverInstance = w.getInstance();
			
			//if hashmap contains server instance
			if(averageServerQueryMap.containsKey(serverInstance)) {
		
				averageServerQueryMap.get(serverInstance).incrementTotalQueries(w.getValue());
				averageServerQueryMap.get(serverInstance).incrementTotalTimeStamps(); 
			}
			else {
				//else create new server instance in hashmap
				averageServerQueryMap.put(serverInstance, new ServerQueryWrapper(w.getValue(),1));
			}
		}
		
		//Format and printing for instance and average queries
		System.out.println("Instance \t\t Average");
		for(String s:averageServerQueryMap.keySet()) {
			System.out.print(s);
			System.out.format("\t %d%n",averageServerQueryMap.get(s).getTotalQueries()/averageServerQueryMap.get(s).getTotalTimeStamps());
		}
		
		
		Timestamp secondTimestamp = new Timestamp(System.currentTimeMillis());
		
		
		
		
		
		
		
		
		
		//3. Calculate the average number of queries that were received on data1 and data2 for all instances
		
		//Data structure that holds interface as key and wrapper class for queries as value
		HashMap<String, ServerQueryWrapper> averageDataQueryMap = new HashMap<String, ServerQueryWrapper>();
		
		for(JSONWrapper w: JSONData) {
			String dataInstance = w.getDataNetwork();
			
			if(averageDataQueryMap.containsKey(dataInstance)){
				averageDataQueryMap.get(dataInstance).incrementTotalQueries(w.getValue());
				averageDataQueryMap.get(dataInstance).incrementTotalTimeStamps();
			}
			else {
				averageDataQueryMap.put(dataInstance,new ServerQueryWrapper(w.getValue(),1));
			}
		}
		
		//Format and printing for interface and average queries
		System.out.println("\nInterface \t\t Average");
		for(String s:averageDataQueryMap.keySet()) {
			System.out.print(s);
			System.out.format("\t\t\t %d%n",averageDataQueryMap.get(s).getTotalQueries()/averageDataQueryMap.get(s).getTotalTimeStamps());
		}
		
		
		
		
		
		
		Timestamp thirdTimestamp = new Timestamp(System.currentTimeMillis());
		
		
		
		//4.Print the number of spikes each instance (both data1 and data2 inclusive) saw. A spike is defined as any value of 700 and up
		
		//Data structure that holds server instance as key and total number of spikes as value
		HashMap<String, Integer> serverSpikeMap = new HashMap<String, Integer>();
		
		for(JSONWrapper w: JSONData){
			String serverInstance = w.getInstance();
			
			//if server instance is already in keyset
			if(serverSpikeMap.containsKey(serverInstance)) {
				if(isSpike(w.getValue())){
					int currentSpikes = serverSpikeMap.get(serverInstance)+1;
					serverSpikeMap.put(serverInstance, currentSpikes);
		
				}	
			}
			
			//if server instance is not in keyset, then add it
			else {
				if(isSpike(w.getValue())){
					serverSpikeMap.put(serverInstance, 1);
				}
				else {
					serverSpikeMap.put(serverInstance, 0);
				}
			}

		}
		//Format and printing for instance and average queries
		System.out.println("\nInstance \t\t Number of spikes");
		for(String s:serverSpikeMap.keySet()) {
			System.out.print(s);
			System.out.format("\t %d%n",serverSpikeMap.get(s));
		}
		
		Timestamp fourthTimestamp = new Timestamp(System.currentTimeMillis());
		
		
		
		
		
		//5.Profile your code. Add timestamps between each step 1 to 4 above and print the time(in any unit you want) it took to output the result
		//Format and printing for timestamps for each step
		System.out.print("\nStep \t\t\t Time");
		System.out.printf("\nStep1 \t\t\t %dms",firstTimestamp.getTime() - startTimestamp.getTime());
		System.out.printf("\nStep2 \t\t\t %dms",secondTimestamp.getTime() - firstTimestamp.getTime());
		System.out.printf("\nStep3 \t\t\t %dms",thirdTimestamp.getTime() - secondTimestamp.getTime());
		System.out.printf("\nStep4 \t\t\t %dms",fourthTimestamp.getTime() - thirdTimestamp.getTime());
		
				
		
		
		

	}
	
	//helper method for determining if queries are a spike, assumes a spike is at least 700 queries
	static boolean isSpike(int spike) {
		int spikeValue = 700;
		if(spike >= spikeValue)return true;
		else return false;
	}

}

//Wrapper class for holding JSON Data
//Assumes data has data network and server instance stored as string, time set in ISO 8601 format and stored as an OffsetDateTime, and value stored as integer
class JSONWrapper{
	
	private String dataNetwork;
	private String instance;
	private OffsetDateTime time;
	private int value;
	
	public JSONWrapper(String dataNetwork, String instance, OffsetDateTime time, int value) {
		this.dataNetwork = dataNetwork;
		this.instance = instance;
		this.time = time;
		this.value = value;
	}
	
	public String getDataNetwork(){return this.dataNetwork;}
	public String getInstance(){return this.instance;}
	public OffsetDateTime getTime(){return this.time;}
	public int getValue(){return this.value;}
	
}

class ServerQueryWrapper{
	
	private int totalQueries;
	private int totalTimeStamps;
	
	public ServerQueryWrapper(int totalQueries, int totalTimeStamps) {
		this.totalQueries = totalQueries;
		this.totalTimeStamps = totalTimeStamps;
	}
	
	public int getTotalQueries() {return this.totalQueries;}
	public int getTotalTimeStamps(){return this.totalTimeStamps;}
	
	public void incrementTotalQueries(int additionalQueries) {this.totalQueries+= additionalQueries;}
	public void incrementTotalTimeStamps() {this.totalTimeStamps++;}
	
	
}

