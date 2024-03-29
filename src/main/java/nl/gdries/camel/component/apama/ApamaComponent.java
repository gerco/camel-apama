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

import java.net.URI;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.apama.services.event.EventServiceFactory;
import com.apama.services.event.IEventService;

/**
 * This class implements the Apama Camel Component. URI should be in the form of
 * apama://host:port/channel,channel2,channel3
 * <p>
 * For a component that is only used for sending events, no channel value is 
 * required, but the trailing slash after port must be given. For example:
 * apama://host:port/
 *  
 * @author Gerco Dries
 */
public class ApamaComponent extends DefaultComponent {

	public static final String PROCESS_NAME_PARAMETER = "processName";
	
	private final Log log = LogFactory.getLog(getClass());
	
	private String defaultProcessName = "Apama Camel Component";
	
	public ApamaComponent() {
	}
	
	public ApamaComponent(CamelContext context) {
		super(context);
	}

	@Override
	protected Endpoint createEndpoint(String uriString, String remaining, Map<String, Object> parameters) throws Exception {
		URI uri = new URI(uriString);

		String processName = parameters.containsKey(PROCESS_NAME_PARAMETER) ?
				(String)parameters.get(PROCESS_NAME_PARAMETER) : defaultProcessName;
		parameters.remove(PROCESS_NAME_PARAMETER);

		log.info("Connecting to " + uriString);
		IEventService eventService = EventServiceFactory.createEventService(
				uri.getHost(), uri.getPort(), processName);
		
		String channels = uri.getPath().substring(1);
		return new ApamaEndpoint(this, eventService, channels);
	}
	
}
