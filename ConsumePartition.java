import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.io.IOException;
import java.io.InputStream;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.PartitionInfo;
import java.util.Arrays;

import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.lang.*;

public class ConsumePartition {
	public static String prettyPrintJsonString(JsonNode jsonNode) {
    		try {
        		ObjectMapper mapper = new ObjectMapper();
        	 	Object json = mapper.readValue(jsonNode.toString(), Object.class);
        			return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
    		} catch (Exception e) {
        		return "Sorry, pretty print didn't work";
    		}
	}

	public static void pollAll(String  TOPIC, KafkaConsumer consumer, Integer pollInterval, String printStr , Integer partId, Integer offset) {

		Integer numRec = 0, totRec = 0;
		Long oldOffset = 0L, newOffset=0L;
		Integer print = 0, printMsg = 0;
		if (printStr.equals("print"))  {
			printMsg = 1;
			print = 1;
		}
		TopicPartition topicPar = new TopicPartition(TOPIC, partId);
		consumer.assign(Arrays.asList(topicPar));
		consumer.seek(topicPar, offset);	
		
		//consumer.subscribe(Arrays.asList(TOPIC));
		while (true) {
			ConsumerRecords<String,String> cr = consumer.poll(pollInterval);
   			numRec = cr.count();
			if ( numRec == 0)
				return;
			for (ConsumerRecord<String,String> record : cr) {
				if (printMsg == 1 ) {
					try {
						ObjectMapper mapper = new ObjectMapper();
						JsonNode msg = mapper.readTree(record.value());
						System.out.printf("%s", prettyPrintJsonString(msg));	
					} catch (Exception e) {
						return;
					}
				}
				newOffset = record.offset();
				if ( print == 1) 
					System.out.printf("offset = %d\n", newOffset);
				if ( oldOffset + 1 != newOffset)
					System.out.printf("Not in seq, offset gap at Old  = %d, New = %d\n", oldOffset, newOffset);	
				oldOffset = newOffset;
			consumer.commitSync();
			}
			System.out.printf("Adding %d records\n", numRec);
			totRec+=numRec;
			numRec=0;
			System.out.println("Total Records: " + totRec ) ;	
		}
        }
    public static void main(String[] args) {


        String TOPIC = "/" + args[0];
		Integer pollInterval = 1000, partId=0, offset=0;
		System.out.println("Topic to be queried is:" +  TOPIC);
		Properties props = new Properties();
		props.put("bootstrap.servers", "");
		props.put("auto.offset.reset", "earliest");
		props.put("enable.auto.commit", "false");
		props.put("max.poll.records", "500");
		System.out.print("Group ID is " + args[1]);
		props.put("group.id",args[1]);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("max.partition.fetch.bytes", Integer.MAX_VALUE);
		

		KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(props);

		
        try {
            // Parse the string argument into an integer value.
            partId = Integer.parseInt(args[2]);
            offset = Integer.parseInt(args[3]);
        }
        catch (NumberFormatException nfe) {
            System.out.println("The first argument must be an integer.");
            System.exit(1);
        }
			pollAll (TOPIC, consumer , pollInterval, "print", partId, offset);
	return;
    }
}
