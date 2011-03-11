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

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;

import com.apama.engine.beans.EngineClientBean;
import com.apama.event.Event;

/**
 * Sends events to Apama. If the body is not a String representing serialized Apama 
 * event, you must register a Camel TypeConverter to convert the Exchange body to an 
 * Apama event. In either case, you must register the relevant EventType with the
 * EventParser by providing an {@link ApamaEventRegistrar}.
 * 
 * @author Gerco Dries
 */
class ApamaProducer extends DefaultProducer {
	private final EngineClientBean engine;
	
	public ApamaProducer(ApamaEndpoint endpoint, EngineClientBean engine) {
		super(endpoint);
		this.engine = engine;
	}

	@Override
	public void process(Exchange exchange) throws Exception {
		Event[] events = new Event[] {exchange.getIn().getBody(Event.class)};
		engine.sendEvents(events);
	}

}
