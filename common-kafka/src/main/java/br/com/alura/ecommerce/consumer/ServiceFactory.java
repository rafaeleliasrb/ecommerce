package br.com.alura.ecommerce.consumer;

public interface ServiceFactory<T> {

	CostumerService<T> create() throws Exception;
}
