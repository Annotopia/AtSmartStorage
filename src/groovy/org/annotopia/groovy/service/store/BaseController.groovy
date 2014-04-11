/*
 * Copyright 2014 Massachusetts General Hospital
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.annotopia.groovy.service.store

import java.io.InputStream;
import java.text.SimpleDateFormat;

import grails.converters.JSON

import javax.servlet.http.HttpServletRequest

/**
 * Basic 
 * 
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class BaseController {

	// Date format for all Open Annotation date content
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssz")
	
	/**
	 * Logging and message for invalid API key.
	 * @param ip	Ip of the client that issued the request
	 */
	public void invalidApiKey(def ip) {
		log.warn("Unauthorized request performed by IP: " + ip)
		def json = JSON.parse('{"status":"rejected" ,"message":"Api Key missing or invalid"}');
		render(status: 401, text: json, contentType: "text/json", encoding: "UTF-8");
	}
	
	/**
	 * Returns the current URL.
	 * @param request 	The HTTP request
	 * @return	The current URL
	 */
	public String getCurrentUrl(HttpServletRequest request){
		StringBuilder sb = new StringBuilder()
		sb << request.getRequestURL().substring(0,request.getRequestURL().indexOf("/", 7))
		sb << request.getAttribute("javax.servlet.forward.request_uri")
		if(request.getAttribute("javax.servlet.forward.query_string")){
			sb << "?"
			sb << request.getAttribute("javax.servlet.forward.query_string")
		}
		return sb.toString();
	}
	
	private InputStream callExternalUrl(String URL) {
		Proxy httpProxy = null;
		if(grailsApplication.config.annotopia.server.proxy.host!=null && grailsApplication.config.annotopia.server.proxy.port!=null) {
			String proxyHost = grailsApplication.config.annotopia.server.proxy.host; //replace with your proxy server name or IP
			int proxyPort = grailsApplication.config.annotopia.server.proxy.port.toInteger(); //your proxy server port
			SocketAddress addr = new InetSocketAddress(proxyHost, proxyPort);
			httpProxy = new Proxy(Proxy.Type.HTTP, addr);
		}
		
		if(httpProxy!=null) {
			long startTime = System.currentTimeMillis();
			log.info ("Proxy request: " + URL);
			URL url = new URL(URL);
			//Pass the Proxy instance defined above, to the openConnection() method
			URLConnection urlConn = url.openConnection(httpProxy);
			urlConn.connect();
			log.info ("Proxy resolved in (" + (System.currentTimeMillis()-startTime) + "ms)");
			return urlConn.getInputStream();
		} else {
			log.info ("No proxy request: " + URL);
			return new URL(URL).openStream();
		}
	}
	
	/**
	 * Creates a JSON message for the response.
	 * @param apiKey	The API key of the client that issued the request
	 * @param status	The status of the response
	 * @param message	The message of the response
	 * @param startTime	The start time to calculate the duration
	 * @return The JSON message
	 */
	public returnMessage(def apiKey, def status, def message, def startTime) {
		log.info("[" + apiKey + "] " + message);
		return JSON.parse('{"status":"' + status + '","message":"' + message +
			'","duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
	}
}
