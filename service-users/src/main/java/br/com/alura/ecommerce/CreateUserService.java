package br.com.alura.ecommerce;

import java.sql.SQLException;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.alura.ecommerce.consumer.CostumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;

public class CreateUserService implements CostumerService<Order> {

	private final LocalDatabase database;
	
	CreateUserService() throws SQLException {
		database = new LocalDatabase("user_database");
		database.createIfNotExists("create table User ("
					+ "uuid varchar(200) primary key,"
					+ "email varchar(200))");
	}
	
	public static void main(String[] args) {
		new ServiceRunner<>(CreateUserService::new).start(1);
	}
	
	@Override
	public String getTopic() {
		return "ECOMMERCE_NEW_ORDER";
	}
	
	@Override
	public String getCosumerGroup() {
		return CreateUserService.class.getSimpleName();
	}

	@Override
	public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
		Order order = record.value().getPayload();
		System.out.println("----------------------------------");
		System.out.println("Processing new order, check for user");
		System.out.println(order);
		
		if(isNewUser(order.getEmail())) {
			insertNewUser(order.getEmail());
		}
	}

	private void insertNewUser(String email) throws SQLException {
		String uuid = UUID.randomUUID().toString();
		database.update("insert into User (uuid, email) values (?, ?)", uuid, email);
		System.out.println("User " + uuid + " e " + email + " added");
	}

	private boolean isNewUser(String email) throws SQLException {
		var result = database.query("select uuid from User where email = ? limit 1", email);
		return !result.next();
	}
}
