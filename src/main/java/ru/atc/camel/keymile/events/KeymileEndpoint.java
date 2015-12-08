package ru.atc.camel.keymile.events;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultPollingEndpoint;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;

@UriEndpoint(scheme="keymile", title="Keymile", syntax="keymile://operationPath", consumerOnly=true, consumerClass=KeymileConsumer.class, label="keymile")
public class KeymileEndpoint extends DefaultPollingEndpoint {

	public KeymileEndpoint(String uri, String operationPath, KeymileComponent component) {
		super(uri, component);
		this.operationPath = operationPath;
	}
	
	private String operationPath;

	@UriParam
	private KeymileConfiguration configuration;

	public Producer createProducer() throws Exception {
		throw new UnsupportedOperationException("OVMMProducer is not implemented");
	}

	@Override
	public Consumer createConsumer(Processor processor) throws Exception {
		KeymileConsumer consumer = new KeymileConsumer(this, processor);
        return consumer;
	}

	public boolean isSingleton() {
		return true;
	}

	public String getOperationPath() {
		return operationPath;
	}

	public void setOperationPath(String operationPath) {
		this.operationPath = operationPath;
	}

	public KeymileConfiguration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(KeymileConfiguration configuration) {
		this.configuration = configuration;
	}
	
}