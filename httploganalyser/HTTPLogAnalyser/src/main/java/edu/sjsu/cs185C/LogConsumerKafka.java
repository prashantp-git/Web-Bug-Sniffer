package edu.sjsu.cs185C;

import org.apache.kafka.clients.consumer.*;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogConsumerKafka {
	
	public static HashMap<String,String> strToHash(String test){
		//System.out.println("Converting:"+test);
		test=test.replace("[", "");
		test=test.replace("(", "");
		test=test.replace(")]", "");
		String[] test2=test.split("\\), ");
		HashMap <String,String> hm=new HashMap<String,String>();
		for (int i=0;i<=test2.length-1;i++) {
			String[] temp;
			temp=test2[i].split(",");
			if(hm.containsKey(temp[0])) {
				hm.replace(temp[0].trim(), temp[1].trim());
			}
			else {
				hm.put(temp[0].trim(), temp[1].trim());
			}		
			
		}
		//System.out.println("Converted"+hm.toString());
		return hm;
	}
	
	public static HashMap<String,String> strToHash2(String test){
		//System.out.println("Converting"+test);
		HashMap <String,String> hm=new HashMap<String,String>();
		test=test.substring(1, test.length()-1);
		String[] pairs=test.split(",");
		for (String st:pairs) {
			String[] entry=st.split("=");
			hm.put(entry[0].trim(), entry[1].trim());
			}
		//System.out.println("Converted"+hm.toString());
		return hm;
	}

	public static void main(String[] args) throws IOException {
		// check command-line arguments
		if (args.length != 2) {
			System.err.println("usage: LogConsumerKafka <input-topic> <cg>");
			System.exit(1);
		}
		String inputTopic = args[0];
		// String groupId = args[1];

		Properties props = new Properties();
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		// props.put("group.id", groupId);
		props.put("bootstrap.servers", "localhost:9092");
		props.put("auto.offset.reset", "earliest");

		// Declare a new consumer.
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		// Subscribe to the topic.
		List<String> topics = new ArrayList<String>();
		topics.add(inputTopic);
		consumer.subscribe(topics);

		// Set the timeout interval for requests for unread messages.
		long pollTimeOut = 1000;

		ConsumerRecords<String, String> consumerRecords = null;
		ConsumerRecord<String, String> record = null;
		Iterator<ConsumerRecord<String, String>> iterator = null;
		
		
		int waitedTime = 0; // seconds
		while (true) {
			// Request unread messages from the topic.
			int count = 0;
			consumerRecords = consumer.poll(pollTimeOut);
			iterator = consumerRecords.iterator();
			MongoClient mongoClntObj = new MongoClient("localhost", 27017);
			while (iterator.hasNext()) {
				record = iterator.next();
				String recKey=record.key();
				String recVal=record.value();
				String tempVal;
				String topic=record.topic();
				HashMap<String,String> newValues,oldValues;
				
				
				//System.out.println(record);
				System.out.println("Record key is"+recKey);
				System.out.println("Record value is"+recVal);
				
				if(!recVal.equals("[]")) {
					System.out.println("Updating value");
					try {
						newValues=strToHash(recVal);
						System.out.println("New Values are"+newValues.toString());
						
						@SuppressWarnings("deprecation")
						DB dbObj = mongoClntObj.getDB("logdata2");
						DBCollection collectionObj = dbObj.getCollection("logcollection");
						BasicDBObject rec=new BasicDBObject();
						BasicDBObject temp;
						rec.append("keyType", recKey);
						//rec.append("keyVal", recVal);
						rec.append("topic", topic);
						rec.append("uniqueKey", topic+"_"+recKey);
						BasicDBObject findDoc=new BasicDBObject();
						findDoc.append("uniqueKey", topic+"_"+recKey);
						if(collectionObj.find(findDoc).count() != 0) {
							
							temp=(BasicDBObject) collectionObj.findOne(findDoc);
							tempVal=temp.getString("keyVal");
							oldValues=strToHash2(tempVal);
							System.out.println("Old values are"+oldValues.toString());
							for (Map.Entry<String, String> s: oldValues.entrySet()) {
								if(!newValues.containsKey(s.getKey())) {
									newValues.put(s.getKey(), s.getValue());
								}
								else {
									//newValues.replace(s.getKey(), s.getValue());
									//Do nothing if value already exist
								}
								
								
							}
							System.out.println("Updated new Values are"+newValues.toString());
							rec.append("keyVal", newValues.toString());
							collectionObj.update(findDoc, rec);
						}
						else {
							System.out.println("Inserting values");
							rec.append("keyVal", newValues.toString());
							collectionObj.insert(rec);
						}
						
						//mongoClntObj.close();
						
						
					}
					catch(MongoException mongoExObj) {
						mongoExObj.printStackTrace();
						System.out.println("Exception"+mongoExObj.getMessage());
						//mongoClntObj.close();
					}
					
					count++;
				}
				}
			mongoClntObj.close();
				
			if (count == 0) {
				waitedTime += 1;
			} else {
				waitedTime = 0; // reset when we get new data
			}
			if (waitedTime > 30) { // stop if no data received for 2 minutes
				break;
			}
		} // end of while(true)
		consumer.close();
	}
}
