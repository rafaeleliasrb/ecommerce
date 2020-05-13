package br.com.alura.ecommerce;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.alura.ecommerce.consumer.CostumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;

public class EmailNewOrderService implements CostumerService<Order> {

	public static void main(String[] args) {
		new ServiceRunner<>(EmailNewOrderService::new).start(1);
	}

	private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>();
	
	@Override
	public void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException {
		var message = record.value();
		Order order = message.getPayload();
		System.out.println("----------------------------------");
		System.out.println("Processing new order, preparing email");
		System.out.println(order);
		System.out.println(record.partition());
		System.out.println(record.offset());
	
		var email = new Email("New order", "Thank you for your order, we are processing your order.");
		emailDispatcher.send("ECOMMERCE_SEND_EMAIL", 
				order.getEmail(), 
				message.getCorrelationId().continueWith(EmailNewOrderService.class.getSimpleName()),
				email);
	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_NEW_ORDER";
	}

	@Override
	public String getCosumerGroup() {
		return EmailNewOrderService.class.getSimpleName();
	}
}
