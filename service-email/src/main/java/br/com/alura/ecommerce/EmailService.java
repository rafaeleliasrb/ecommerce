package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.alura.ecommerce.consumer.CostumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;

public class EmailService implements CostumerService<Email> {

	public static void main(String[] args) {
		new ServiceRunner<>(EmailService::new).start(5);
	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_SEND_EMAIL";
	}

	@Override
	public String getCosumerGroup() {
		return EmailService.class.getSimpleName();
	}

	@Override
	public void parse(ConsumerRecord<String, Message<Email>> record) {
		System.out.println("----------------------------------");
		System.out.println("Sending email");
		System.out.println(record.key());
		System.out.println(record.value().getPayload());
		System.out.println(record.partition());
		System.out.println(record.offset());
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO 
			e.printStackTrace();
		}
		System.out.println("Email sent");
	}
	
}
