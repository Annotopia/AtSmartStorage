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
package org.annotopia.grails.services.storage.authentication

import java.util.regex.Matcher

/**
 * This service manages access through API keys that are assigned to 
 * applications or users that want to make use of the Smart Storage.
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class ApiKeyAuthenticationService {

	def grailsApplication
	def configAccessService
	
	/**
	 * Returns true if the tested apiKey is valid. At the moment this validates
	 * the testing ApiKey only. Later it will test the validity against real 
	 * API keys.
	 * @param apiKey	The ApiKey assigned to a client
	 * @return True if the client is authorized
	 */
	def isApiKeyValid(def ip, def apiKey) {
		log.info("Validating API key [" + apiKey + "] on request from IP: " + ip);
		// Validation mockup for testing mode
		boolean allowed = (
			configAccessService.getAsString("annotopia.storage.testing.enabled")=='true' &&
			apiKey==configAccessService.getAsString("annotopia.storage.testing.apiKey")
		);
	 	return allowed;
	}
	
	/**
	 * Returns the API key used to authorize the request. First checks the "Authorization" header,
	 * then checks for a URL parameter named "apiKey". Returns null if no API key found.
	 * @param request	The HTTP request object
	 * @return API key in a String, or null
	 */
	def getApiKey(def request) {
		Matcher matcher = request.getHeader("Authorization") =~ /.*annotopia-api-key\s+([-0-9a-fA-F]*).*/
		if(matcher.matches())
			return matcher.group(1)
		else
			return request.getParameter("apiKey")
	}
}
