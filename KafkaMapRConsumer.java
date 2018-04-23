import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author Dinesh Thangaraj
 *
 * Created on 18-Apr-2018
 */
public class KafkaMapRConsumer {

	public static void main(String[] args) throws Exception {

		String topic = "XYZ";

		Properties props = new Properties();
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE);
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		List<String> topics = Arrays.asList(new String[] { topic });
		consumer.subscribe(topics);
		long pollTimeout = 1000;
		while (true) {
			ConsumerRecords<String, String> consumerRecords = consumer.poll(pollTimeout);
			Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
			if (iterator.hasNext()) {
				while (iterator.hasNext()) {
					ConsumerRecord<String, String> record = iterator.next();
					System.out.println(("Consumed Record: " + record.toString()));
				}
			}
			consumer.commitSync();
		}
		// close consumer here
	}

}
