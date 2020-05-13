package br.com.alura.ecommerce;

import java.lang.reflect.Type;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

@SuppressWarnings("rawtypes")
public class MessageAdapter implements JsonSerializer<Message>, JsonDeserializer<Message> {

	public static final String TYPE = "type";
	public static final String CORRELATION_ID = "correlationId";
	public static final String PAYLOAD = "payload";
	
	@Override
	public JsonElement serialize(Message message, Type typeOfSrc, JsonSerializationContext context) {
		var obj = new JsonObject();
		obj.addProperty(TYPE, message.getPayload().getClass().getName());
		obj.add(CORRELATION_ID, context.serialize(message.getCorrelationId()));
		obj.add(PAYLOAD, context.serialize(message.getPayload()));
		return obj;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Message deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) {
		var obj = json.getAsJsonObject();
		var payloadType = obj.get(TYPE).getAsString();
		var correlationId = (CorrelationId) context.deserialize(obj.get(CORRELATION_ID), CorrelationId.class);
		
		try {
			var payload = context.deserialize(obj.get(PAYLOAD), Class.forName(payloadType));
			return new Message(correlationId, payload);
		} catch (JsonParseException | ClassNotFoundException e) {
			throw new JsonParseException(e);
		}
	}

}
