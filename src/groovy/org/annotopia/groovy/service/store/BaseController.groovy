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
