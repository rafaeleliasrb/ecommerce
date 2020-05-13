package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;

public class NewOrderMain {
    
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        try(var orderDispatcher = new KafkaDispatcher<Order>()) {
    		var emailAddress = Math.random() + "@email.com";
	    	for(int i=0; i<10; i++) {
		        var order = new Order(UUID.randomUUID().toString(), BigDecimal.valueOf(Math.random()*5000 + 1), emailAddress);
				
				orderDispatcher.send("ECOMMERCE_NEW_ORDER", 
						emailAddress, 
						new CorrelationId(NewOrderMain.class.getSimpleName()),
						order);
	        }
        }
    }
}
