package br.com.alura.ecommerce.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.alura.ecommerce.Message;

public interface CostumerService<T> {

	public String getTopic();
	public String getCosumerGroup();
	public void parse(ConsumerRecord<String, Message<T>> record) throws Exception;
}
