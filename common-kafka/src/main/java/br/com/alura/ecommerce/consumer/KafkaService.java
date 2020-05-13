package br.com.alura.ecommerce.consumer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import br.com.alura.ecommerce.Message;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;

public class KafkaService<T> implements Closeable {

	private final KafkaConsumer<String, Message<T>> consumer;
	private final ConsumerFunction<T> parse;

	public KafkaService(String groupId, String topico, ConsumerFunction<T> parse, Map<String, String> overrideProperties) {
		this(groupId, parse, overrideProperties);
		consumer.subscribe(Collections.singletonList(topico));
	}

	public KafkaService(String groupId, Pattern topico, ConsumerFunction<T> parse, Map<String, String> overrideProperties) {
		this(groupId, parse, overrideProperties);
		consumer.subscribe(topico);
	}

	private KafkaService(String groupId, ConsumerFunction<T> parse, Map<String, String> overrideProperties) {
		this.parse = parse;
		consumer = new KafkaConsumer<>(properties(groupId, overrideProperties));
	}
	
	public void run() throws InterruptedException, ExecutionException {
		try(var deadLetter = new KafkaDispatcher<>()) {
			while(true) {
				var records = consumer.poll(Duration.ofMillis(100));
				if(!records.isEmpty()) {
					System.out.println(records.count() + " records were found");
					for(var record : records) {
						try {
							parse.consume(record);
						} catch (Exception e) {
							var message = record.value();
							deadLetter.send("ECOMMERCE_DEADLETTER",
									message.getCorrelationId().toString(),
									message.getCorrelationId().continueWith("DeadLetter"),
									message/* new GsonSerializer().serialize("", message) */);
						}
					}
				}
			}
		}
	}

	private Properties properties(String groupId, Map<String, String> overrideProperties) {
		var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.putAll(overrideProperties);
		return properties;
	}

	@Override
	public void close() {
		consumer.close();
	}
}
