package br.com.alura.ecommerce.dispatcher;

import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import br.com.alura.ecommerce.Message;
import br.com.alura.ecommerce.MessageAdapter;

@SuppressWarnings("rawtypes")
public class GsonSerializer implements Serializer<Message> {

	private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdapter()).create();

	@Override
	public byte[] serialize(String topic, Message data) {
		return gson.toJson(data).getBytes();
	}
	
}
