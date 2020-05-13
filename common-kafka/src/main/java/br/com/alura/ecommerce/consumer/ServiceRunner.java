package br.com.alura.ecommerce.consumer;

import java.util.concurrent.Executors;

public class ServiceRunner<T> {

	private ServiceProvider<T> provider;

	public ServiceRunner(ServiceFactory<T> factory) {
		this.provider = new ServiceProvider<>(factory);
	}
	
	public void start(int nThreads) {
		var threadPool = Executors.newFixedThreadPool(nThreads);
		for(int i = 0; i < nThreads; i++) {
			threadPool.submit(provider);
		}
	}

}
