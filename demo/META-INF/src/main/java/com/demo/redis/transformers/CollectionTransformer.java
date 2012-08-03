package com.demo.redis.transformers;

import java.io.UnsupportedEncodingException;
import java.util.Set;

import org.mule.api.MuleMessage;
import org.mule.api.transformer.TransformerException;
import org.mule.transformer.AbstractMessageTransformer;

public class CollectionTransformer extends AbstractMessageTransformer {

	private String SPLITTER = ",";
	@Override
	public Object transformMessage(MuleMessage message, String outputEncoding)
			throws TransformerException {
		
		Set<byte[]> payload = (Set<byte[]>) message.getPayload();
		
		StringBuilder sb = new StringBuilder();

		try {
			for (byte[] val : payload) {
				sb.append(new String(val, "UTF8"));
				sb.append(SPLITTER);
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		return sb;
		
	}

}
