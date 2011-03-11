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
import java.util.ServiceLoader;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.apama.engine.beans.EngineClientBean;
import com.apama.event.parser.EventParser;
import com.apama.util.CompoundException;

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
	
	private ScheduledExecutorService pings = Executors.newScheduledThreadPool(1);

	{
		// Have all Apama Events registered with the parser by their registrars
		setupEventParser();
	}
	
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

		EngineClientBean engine = new EngineClientBean();
		engine.setHost(uri.getHost());
		engine.setPort(uri.getPort());
		engine.setProcessName(processName);
		engine.setConnectionPollingInterval(10000);

		pings.scheduleWithFixedDelay(new PingEngine(engine), 0, 10, TimeUnit.SECONDS);
		
		String[] channels = uri.getPath().substring(1).split(",");
		return new ApamaEndpoint(this, engine, channels);
	}

	private void setupEventParser() {
		EventParser parser = EventParser.getDefaultParser();
		
		ServiceLoader<ApamaEventRegistrar> registrars = ServiceLoader.load(ApamaEventRegistrar.class);
		for(ApamaEventRegistrar registrar: registrars) {
			registrar.registerEventTypes(parser);
		}
	}
	
	@Override
	protected void doStop() throws Exception {
		log.debug("Shutting down ping thread");
		pings.shutdown();
		super.doStop();
	}
	
	private class PingEngine implements Runnable {
		private EngineClientBean engine;
		
		public PingEngine(EngineClientBean engine) {
			this.engine = engine;
		}
		
		@Override
		public void run() {
			try {
				boolean wasConnected = engine.isBeanConnected();
				engine.pingServer();
				if(!wasConnected)
					log.info("Successfully connected to correlator at " + engine.getHost() + ":" + engine.getPort());
			} catch (CompoundException e) {
				log.warn("Could not connect to correlator at " + engine.getHost() + ":" + engine.getPort());
			}
		}
		
	}

}
