package br.com.alura.ecommerce.dispatcher;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import br.com.alura.ecommerce.CorrelationId;
import br.com.alura.ecommerce.Message;

public class KafkaDispatcher<T> implements Closeable {

	private final KafkaProducer<String, Message<T>> producer;

	public KafkaDispatcher() {
		this.producer = new KafkaProducer<>(properties());
	}
	
	public void send(String topico, String key, CorrelationId correlationId, T payload) 
			throws InterruptedException, ExecutionException {
		var future = sendAsync(topico, key, correlationId, payload);
		
		future.get();
	}

	public Future<RecordMetadata> sendAsync(String topico, String key, CorrelationId correlationId, T payload) {
		var message = new Message<T>(correlationId.continueWith("_" + topico), payload);
		var produceRecord = new ProducerRecord<String, Message<T>>(topico, key, message);
		Callback callback = (data, ex) -> {
			if(ex != null) {
				ex.printStackTrace();
				return;
			}
			
			System.out.println("sucesso enviado " + data.topic() + "::partition: " + data.partition() + " /offset: " + data.offset() +
					"/timestamp: " + data.timestamp());
		};
		
		return producer.send(produceRecord, callback);
	}
	
	private static Properties properties() {
		var properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		return properties;
	}

	@Override
	public void close() {
		producer.close();
	}

}
