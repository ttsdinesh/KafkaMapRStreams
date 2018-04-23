import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author Dinesh Thangaraj
 *
 * Created on 18-Apr-2018
 */
public class KafkaMapRProducer {

	public static void main(String[] args) throws Exception {

		String topic = "XYZ";

		Properties props = new Properties();
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		for (int i = 0; i < 10; i++) {
			String messageText = "Msg " + i;
			ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, String.valueOf(i),
					messageText);
			producer.send(rec);
			System.out.println("Sent message number " + i);
		}
		producer.close();
	}
}
