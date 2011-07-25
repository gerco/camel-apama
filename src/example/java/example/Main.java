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
package example;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;

/**
 * Example for usage as a stand-alone application.
 * 
 * @author Gerco Dries
 */
public class Main {

	public static void main(String[] args) throws Exception {
		CamelContext ctx = new DefaultCamelContext();

		ctx.addRoutes(new RouteBuilder() {
			@Override
			public void configure() throws Exception {
				from("apama://localhost:15903/TEST")
					.setHeader("correlator", constant(1))
					.to("seda:requests");
				
//				from("apama://localhost:15904/channel")
//					.setHeader("correlator", constant(2))
//					.to("seda:requests");
				
				from("seda:requests?concurrentConsumers=5")
					.bean(new DebugBean(), "debug")
					.to("seda:responses");
				
				from("seda:responses")
					.choice()
						.when(header("correlator").isEqualTo(1))
							.to("apama://localhost:15903/");
//						.when(header("correlator").isEqualTo(2))
//							.to("apama://localhost:15904/");
			}
		});

		ctx.start();
		
		Thread.sleep(Integer.MAX_VALUE);
	}

}
