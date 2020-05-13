package br.com.alura.ecommerce;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.alura.ecommerce.consumer.CostumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;

public class ReadingReportService implements CostumerService<User>{

	private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();
	
	public static void main(String[] args) {
		new ServiceRunner<>(ReadingReportService::new).start(5);
	}

	@Override
	public void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
		var message = record.value();
		User user = message.getPayload();
		System.out.println("----------------------------------");
		System.out.println("Processing report for: " + user.getUuid());
		
		var target = new File(user.getReportPath());
		IO.copyTo(SOURCE, target);
		IO.append(target, "Created for: " + user.getUuid());
		
		System.out.println("File created at: " + target.getAbsolutePath());
	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_USER_GENERATE_READING_REPORT";
	}

	@Override
	public String getCosumerGroup() {
		return ReadingReportService.class.getSimpleName();
	}
}
