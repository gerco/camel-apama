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

import java.util.UUID;

import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.apache.camel.impl.DefaultExchange;
import org.apache.camel.impl.DefaultMessage;

import com.apama.EngineException;
import com.apama.engine.beans.EngineClientBean;
import com.apama.engine.beans.interfaces.ConsumerOperationsInterface;
import com.apama.event.Event;
import com.apama.event.EventListenerAdapter;

/**
 * A consumer for Apama events. Subscribes to a list of channels and sends
 * received events off to Camel to be handled. This class adds two headers
 * to the Exchanges being sent:
 * <p>
 * <ul>
 *   <li><b>channel</b>: The Apama channel the message was received from.</li>
 *   <li><b>eventType</b>: The Apama event type of the Exchange body.</li>
 * </ul>
 * @author Gerco Dries
 */
class ApamaConsumer extends DefaultConsumer {
	private final UUID uuid = UUID.randomUUID();
	private final EngineClientBean engine;
	private final ConsumerOperationsInterface consumer;
	private final EventListener eventListener = new EventListener();
	
	public ApamaConsumer(Endpoint endpoint, Processor processor, EngineClientBean engine, String[] channels) throws EngineException {
		super(endpoint, processor);
		this.engine = engine;
		consumer = engine.addConsumer(uuid.toString(), channels);
		consumer.addEventListener(eventListener);
	}

	private void handleEvent(Event event) {
		Exchange ex = new DefaultExchange(getEndpoint());
		ex.setIn(new DefaultMessage());
		setHeaders(ex.getIn(), event);
		ex.getIn().setBody(event);
		try {
			getProcessor().process(ex);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void setHeaders(Message message, Event event) {
		message.setHeader("channel", event.getChannel());
		message.setHeader("eventType", event.getName());
	}

	@Override
	protected void doStop() throws Exception {
		super.doStop();
		log.debug("Removing eventlistener");
		consumer.removeEventListener(eventListener);
		log.debug("Removing consumer " + uuid);
		engine.removeConsumer(uuid.toString());
	}
	
	private class EventListener extends EventListenerAdapter {
		
		@Override
		public void handleEvent(Event event) {
			ApamaConsumer.this.handleEvent(event);
		}
		
	}
	
	
}
