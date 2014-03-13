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
package org.annotopia.grails.controllers.annotation.openannotation

import org.annotopia.groovy.service.store.BaseController;
import org.apache.commons.collections.functors.WhileClosure;

/**
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class OpenAnnotationReportingController extends BaseController {

	def apiKeyAuthenticationService;
	def openAnnotationReportingService;
	def jenaVirtuosoStoreService;
	
	def countAnnotations = {
		long startTime = System.currentTimeMillis();
		
		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}
		int count = openAnnotationReportingService.countAnnotations(apiKey);
		render count;
	}
	
	def countAnnotationSets = {
		long startTime = System.currentTimeMillis();
		
		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}

		response.contentType = "text/json;charset=UTF-8";
		try {						
			int count = openAnnotationReportingService.countAnnotationSets(apiKey);			
			response.outputStream << '{"status":"results", "result": {"countAnnotationSets":';
			response.outputStream << '"' + count + '"';
			response.outputStream << '}}';
			response.outputStream.flush();
		} catch(Exception e) {
			log.error("[" + apiKey + "] " + e.getMessage())
			def message = 'Counting of Annotation Sets not completed';
			render(status: 500, text: returnMessage(apiKey, "failure", message, startTime),
				contentType: "text/json", encoding: "UTF-8");
			return;
		}
	}
	
	def countAnnotatedResources = {
		long startTime = System.currentTimeMillis();
		
		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}
		
		int count = openAnnotationReportingService.countAnnotatedResources(apiKey);
		render count;
	}
	
	def countAnnotatedInFullResources = {
		long startTime = System.currentTimeMillis();
		
		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}
		
		int count = openAnnotationReportingService.countAnnotatedInFullResources(apiKey);
		render count;
	}
	
	def countAnnotatedInPartResources = {
		long startTime = System.currentTimeMillis();
		
		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}
		
		int count = openAnnotationReportingService.countAnnotatedInPartResources(apiKey);
		render count;
	}
	
	def countAnnotationsForEachResource = {
		long startTime = System.currentTimeMillis();
		
		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}

		response.contentType = "text/json;charset=UTF-8";
		try {
			int count = openAnnotationReportingService.countAnnotationSets(apiKey);
			response.outputStream << '{"status":"results", "result": {"annotationsForEachResource":[';
			
			Map map = openAnnotationReportingService.countAnnotationsForAllResources(apiKey);
			Set<String> resources = map.keySet();
			resources.eachWithIndex { resource, i ->
				response.outputStream << '{"target":"' + resource + '",';
				response.outputStream << '"annotations":"' + map.get(resource) + '"}';
				if(i<map.size()-1) response.outputStream << ',';
			}			
			response.outputStream << ']}}';
			response.outputStream.flush();
		} catch(Exception e) {
			log.error("[" + apiKey + "] " + e.getMessage())
			def message = 'Counting of Annotations for each Resource not completed';
			render(status: 500, text: returnMessage(apiKey, "failure", message, startTime),
				contentType: "text/json", encoding: "UTF-8");
			return;
		}
	}
}
