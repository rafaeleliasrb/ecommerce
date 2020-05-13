package br.com.alura.ecommerce;

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.alura.ecommerce.consumer.CostumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;

public class FraudDetectorService implements CostumerService<Order> {

	private final LocalDatabase database;
	
	FraudDetectorService() throws SQLException {
		database = new LocalDatabase("frauds_database");
		database.createIfNotExists("create table Orders ("
				+ "uuid varchar(200) primary key, "
				+ "is_fraud boolean)");
	}
	
	
	public static void main(String[] args) {
		new ServiceRunner<>(FraudDetectorService::new).start(1);
	}

	
	@Override
	public String getTopic() {
		return "ECOMMERCE_NEW_ORDER";
	}
	
	@Override
	public String getCosumerGroup() {
		return FraudDetectorService.class.getSimpleName();
	}
	
	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
	
	@Override
	public void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException, SQLException {
		var message = record.value();
		Order order = message.getPayload();
		System.out.println("----------------------------------");
		System.out.println("Processing new order, check for frauds");
		System.out.println(record.key());
		System.out.println(order);
		System.out.println(record.partition());
		System.out.println(record.offset());
		
		if(wasProcessed(order)) {
			System.out.println("Order " + order.getOrderId() + " was already processed.");
			return;
		}
		
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO 
			e.printStackTrace();
		}
		
		if(order.isFraud()) {
			database.update("insert into Orders (uuid, is_fraud) values (?, true)", order.getOrderId());
			System.out.println("It is a fraud! " + order);
			orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", 
					order.getEmail(), 
					message.getCorrelationId().continueWith(FraudDetectorService.class.getSimpleName()),
					order);
		}
		else {
			database.update("insert into Orders (uuid, is_fraud) values (?, false)", order.getOrderId());
			System.out.println("It is ok, it is not a fraud. " + order);
			orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", 
					order.getEmail(), 
					message.getCorrelationId().continueWith(FraudDetectorService.class.getSimpleName()),
					order);
		}
	}

	private boolean wasProcessed(Order order) throws SQLException {
		var results = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
		return results.next();
	}
}
