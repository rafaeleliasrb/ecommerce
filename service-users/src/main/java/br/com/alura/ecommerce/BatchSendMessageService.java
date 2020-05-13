package br.com.alura.ecommerce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.alura.ecommerce.consumer.KafkaService;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;

public class BatchSendMessageService {

	private final Connection connection;
	
	public BatchSendMessageService() throws SQLException {
		var url = "jdbc:sqlite:target/user_database.db";
		connection = DriverManager.getConnection(url);
	}
	
	public static void main(String[] args) throws SQLException, InterruptedException, ExecutionException {
		var createUserService = new BatchSendMessageService();
		try(var service = new KafkaService<String>(BatchSendMessageService.class.getSimpleName(), 
				"ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS", 
				createUserService::parse, 
				new HashMap<>())) {
			service.run();
		}
	}

	private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();
	
	private void parse(ConsumerRecord<String, Message<String>> record) throws SQLException, InterruptedException, ExecutionException {
		var message = record.value();
		System.out.println("----------------------------------");
		System.out.println("Processing new batch");
		System.out.println("Topic: " + message.getPayload());
		
		for(User user: getAllUsers()) {
			userDispatcher.sendAsync(message.getPayload(), 
					user.getUuid(), 
					message.getCorrelationId().continueWith(BatchSendMessageService.class.getSimpleName()), 
					user);
			System.out.println("Enviei para: " + user);
		}
	}

	private Set<User> getAllUsers() throws SQLException {
		var users = new HashSet<User>();
		var select = connection.prepareStatement("select uuid from User").executeQuery();
		while(select. next()) {
			users.add(new User(select.getString(1)));
		}
		
		return users;
	}
}
