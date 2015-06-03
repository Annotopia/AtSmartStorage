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

import grails.converters.JSON
import groovy.sql.DataSet

import java.text.SimpleDateFormat

import javax.servlet.http.HttpServletRequest

import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFLanguages

import com.hp.hpl.jena.rdf.model.Model

/**
 * Basic methods for storage controllers.
 *
 * Note: the references to grailsApplication and log work because of inheritance with the actual controller.
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class BaseController {

	// Date format for all Open Annotation date content
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")

	/**
	 * Logging and message for invalid API key.
	 * @param ip	Ip of the client that issued the request
	 */
	public void invalidApiKey(def ip) {
		log.warn("Unauthorized request performed by IP: " + ip)
		def json = JSON.parse('{"status":"rejected" ,"message":"Api Key missing or invalid"}');
		response.setHeader('WWW-Authenticate', 'annotopia-api-key');
		render(status: 401, text: json, contentType: "text/json", encoding: "UTF-8");
	}

	/**
	 * Returns the current URL.
	 * @param request 	The HTTP request
	 * @return	The current URL
	 */
	public String getCurrentUrl(HttpServletRequest request){
		def url = configAccessService.getAsString("grails.server.protocol") + "://" +
				configAccessService.getAsString("grails.server.host") + ":" +
				configAccessService.getAsString("grails.server.port") +
				request.forwardURI; // The path
		if(request.getAttribute("javax.servlet.forward.query_string")){ // The query
			url += "?"
			url += request.getAttribute("javax.servlet.forward.query_string")
		}

		return url;
	}

	/**
	 * Method for calling external URLs with or without proxy.
	 * @param agentKey 	The agent key for logging
	 * @param URL		The external URL to call
	 * @return The InputStream of the external URL.
	 */
	private InputStream callExternalUrl(def agentKey, String URL) {
		Proxy httpProxy = null;
		if(grailsApplication.config.annotopia.server.proxy.host && grailsApplication.config.annotopia.server.proxy.port) {
			String proxyHost = configAccessService.getAsString("annotopia.server.proxy.host"); //replace with your proxy server name or IP
			int proxyPort = configAccessService.getAsString("annotopia.server.proxy.port").toInteger(); //your proxy server port
			SocketAddress addr = new InetSocketAddress(proxyHost, proxyPort);
			httpProxy = new Proxy(Proxy.Type.HTTP, addr);
		}

		if(httpProxy!=null) {
			long startTime = System.currentTimeMillis();
			logInfo(agentKey, "Proxy request: " + URL);
			URL url = new URL(URL);
			URLConnection urlConn = url.openConnection(httpProxy);
			urlConn.connect();
			logInfo(agentKey, "Proxy resolved in (" + (System.currentTimeMillis()-startTime) + "ms)");
			return urlConn.getInputStream();
		} else {
			logInfo(agentKey, "No proxy request: " + URL);
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

	public String getDatasetAsString(DataSet dataset) {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		RDFDataMgr.write(outputStream, dataset, RDFLanguages.JSONLD);
		return outputStream.toString();
	}

	public String getDatasetAsString(Model model) {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		RDFDataMgr.write(outputStream, model, RDFLanguages.JSONLD);
		return outputStream.toString();
	}

	private def logInfo(def userId, message) {
		log.info(":" + userId + ": " + message);
	}

	private def logDebug(def userId, message) {
		log.debug(":" + userId + ": " + message);
	}

	private def logWarning(def userId, message) {
		log.warn(":" + userId + ": " + message);
	}

	private def logException(def userId, message) {
		log.error(":" + userId + ": " + message);
	}
}
