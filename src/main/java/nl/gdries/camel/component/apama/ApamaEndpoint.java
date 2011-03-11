/**
 * Copyright 2011 Gerco Dries. All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are
 * permitted provided that the following conditions are met:
 * 
 *    1. Redistributions of source code must retain the above copyright notice, this list of
 *       conditions and the following disclaimer.
 * 
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL GERCO DRIES OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package nl.gdries.camel.component.apama;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.apama.engine.beans.EngineClientBean;

class ApamaEndpoint extends DefaultEndpoint {
	private final EngineClientBean engine;
	private final String[] channels;
	private final Log log = LogFactory.getLog(getClass());
	private ApamaConsumer consumer;
	
	public ApamaEndpoint(ApamaComponent component, EngineClientBean engine, String[] channels) {
		this.engine = engine;
		this.channels = channels;
		this.setCamelContext(component.getCamelContext());
	}
	
	@Override
	public synchronized Consumer createConsumer(Processor processor) throws Exception {
		if(consumer == null)
			consumer = new ApamaConsumer(this, processor, getEngine(), getChannels());
		return consumer;
	}

	@Override
	public synchronized Producer createProducer() throws Exception {
		return new ApamaProducer(this, getEngine());
	}

	@Override
	public boolean isSingleton() {
		return false;
	}

	@Override
	protected String createEndpointUri() {
		return "apama://" + engine.getHost() + ":" + engine.getPort() + "/" + getChannelsAsString();
	}


	public EngineClientBean getEngine() {
		return engine;
	}

	public String[] getChannels() {
		return channels;
	}

	private String getChannelsAsString() {
		StringBuilder builder = new StringBuilder();
		for(String c: getChannels()) {
			builder.append(',');
			builder.append(c);
		}
		return builder.substring(1);
	}
	
	@Override
	public void start() throws Exception {
		log.info("Starting apama endpoint " + createEndpointUri());
		super.start();
	}
	
	@Override
	public void stop() throws Exception {
		log.info("Stopping apama endpoint " + createEndpointUri());
		super.stop();
		engine.disconnect();
	}
	
}
