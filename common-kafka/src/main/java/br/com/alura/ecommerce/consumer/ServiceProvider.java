package br.com.alura.ecommerce.consumer;

import java.util.HashMap;
import java.util.concurrent.Callable;

public class ServiceProvider<T> implements Callable<Void>{

	private ServiceFactory<T> factory;

	ServiceProvider(ServiceFactory<T> factory) {
		this.factory = factory;
	}
	
	@Override
	public Void call() throws Exception {
		var myService = factory.create();
		try(var service = new KafkaService<T>(myService.getCosumerGroup(), 
				myService.getTopic(), 
				myService::parse, 
				new HashMap<>())) {
			service.run();
		}
		
		return null;
	}

}
