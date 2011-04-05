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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.TypeConverter;
import org.apache.camel.impl.DefaultProducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.apama.engine.beans.EngineClientBean;
import com.apama.event.Event;

/**
 * Sends events to Apama. If the body is not one of the formats in the list below, you 
 * must register a Camel TypeConverter to convert the Exchange body to an
 * Apama event. In either case, you must register the relevant EventType with the
 * EventParser by providing an {@link ApamaEventRegistrar}.
 * <p>
 * Supported input types:
 * <ul>
 * <li>Message bodies that can be converted into Event objects;</li>
 * <li>java.util.List that contains objects that can be converted into Event objects;</li>
 * <li>Grouped Exchanges with any of the above body types. The body of the grouped Exchange itself will be ignored;</li>
 * </ul>
 * 
 * @author Gerco Dries
 *
 */
class ApamaProducer extends DefaultProducer {
	private final Log log = LogFactory.getLog(getClass());
	private final EngineClientBean engine;
	
	private TypeConverter typeConverter;
	
	public ApamaProducer(ApamaEndpoint endpoint, EngineClientBean engine) {
		super(endpoint);
		this.engine = engine;
		this.typeConverter = getEndpoint().getCamelContext().getTypeConverter();
	}

	@Override
	public void process(Exchange exchange) throws Exception {
		Event[] events = convertExchange(exchange);
		
		if(events.length > 0) {
			if(log.isDebugEnabled())
				log.debug(String.format("Sending %d events: %s", events.length, Arrays.toString(events)));
			engine.sendEvents(true, events);
		}
	}

	@SuppressWarnings("unchecked")
	private Event[] convertExchange(Exchange exchange) {
		List<Exchange> group = (List<Exchange>) 
			exchange.getProperty(Exchange.GROUPED_EXCHANGE, List.class);
		if(group != null) {
			return convertGroupedExchange(group);
		} else {
			return convertSingleExchange(exchange);
		}
	}
	
	private Event[] convertGroupedExchange(List<Exchange> group) {
		List<Event> events = new ArrayList<Event>();
		for(Exchange ex: group) {
			for(Event evt: convertSingleExchange(ex)) {
				events.add(evt);
			}
		}
		return events.toArray(new Event[events.size()]);
	}

	@SuppressWarnings("unchecked")
	private Event[] convertSingleExchange(Exchange e) {
		Object body = e.getIn().getBody();
		if(body instanceof List) {
			return convertListOfEvents((List<Object>)body);
		} else {
			return new Event[] {convertSingleEvent(body)};
		}
	}

	private Event[] convertListOfEvents(List<Object> objects) {
		Event[] events = new Event[objects.size()];
		int i = 0;
		for(Object object: objects) {
			events[i++] = convertSingleEvent(object);
		}
		return events;
	}

	private Event convertSingleEvent(Object object) {
		return typeConverter.convertTo(Event.class, object);
	}

}
