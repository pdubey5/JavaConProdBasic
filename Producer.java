import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This producer will send a bunch of messages to topic "fast-messages". Every so often,
 * it will send a message to "slow-messages". This shows how messages can be sent to
 * multiple topics. On the receiving end, we will see both kinds of messages but will
 * also see how the two topics aren't really synchronized.
 */
public class Producer {
    public static void main(String[] args) throws IOException {

	String TOPIC = "/" + args[0];
        Long numMessages = 0L;
	System.out.println(TOPIC);
	try {
            // Parse the string argument into an integer value.
            numMessages = Long.parseLong(args[1]);
        }
        catch (NumberFormatException nfe) {
            // The first argument isn't a valid integer.  Print
            // an error message, then exit with an error code.
            System.out.println("The first argument must be an integer.");
            System.exit(1);
        }

/*        TOPIC[0] = "/str_seek:topic_1";
        TOPIC[1] = "/str_seek:topic_2";
        TOPIC[2] = "/str_seek:topic_3";
        TOPIC[3] = "/str_seek:topic_4";
        TOPIC[4] = "/str_seek:topic_0";
        TOPIC[5] = "/str_seek:topic_5";
        TOPIC[6] = "/str_seek:topic_6";
        TOPIC[7] = "/str_seek:topic_7";
        TOPIC[8] = "/str_seek:topic_8";
        TOPIC[9] = "/str_seek:topic_9";
 */       // set up the producer
	KafkaProducer<String, String> producer;
        Properties props = new Properties();
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("enable.idempotence", "true");
        props.put("block.on.buffer.full", "true");
        producer = new KafkaProducer<>(props);
      
      try {
		for (int i = 0; i < numMessages; i++) {
                // send lots of messages
                producer.send(new ProducerRecord<String, String>(
                        TOPIC,
                        String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
              /*  producer.send(new ProducerRecord<String, String>(
                        TOPIC[0],
                        String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
                producer.send(new ProducerRecord<String, String>(
                        TOPIC[1],
                        String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
                producer.send(new ProducerRecord<String, String>(
                        TOPIC[2],
                        String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
                producer.send(new ProducerRecord<String, String>(
                        TOPIC[3],
                        String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
                producer.send(new ProducerRecord<String, String>(
                        TOPIC[4],
                        String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
                producer.send(new ProducerRecord<String, String>(
                        TOPIC[5],
                        String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
                producer.send(new ProducerRecord<String, String>(
                        TOPIC[6],
                        String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
                producer.send(new ProducerRecord<String, String>(
                        TOPIC[7],
                        String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
                producer.send(new ProducerRecord<String, String>(
                        TOPIC[8],
                        String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
                producer.send(new ProducerRecord<String, String>(
                        TOPIC[9],
                        String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)));
*/
                    System.out.println("Sent msg number " + i);
            }
        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
        }

    }
}
