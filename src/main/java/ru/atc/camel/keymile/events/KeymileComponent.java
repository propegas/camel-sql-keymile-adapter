package ru.atc.camel.keymile.events;

import java.util.Map;

import org.apache.camel.Endpoint;
import org.apache.camel.impl.UriEndpointComponent;

public class KeymileComponent extends UriEndpointComponent {

	public KeymileComponent() {
		super(KeymileEndpoint.class);
	}

	@Override
	protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
		
		KeymileEndpoint endpoint = new KeymileEndpoint(uri, remaining, this);		
		KeymileConfiguration configuration = new KeymileConfiguration();
		
		// use the built-in setProperties method to clean the camel parameters map
		setProperties(configuration, parameters);
		
		endpoint.setConfiguration(configuration);		
		return endpoint;
	}
}