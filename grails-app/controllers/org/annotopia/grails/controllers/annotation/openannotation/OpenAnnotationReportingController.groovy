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
 * This controller gives some basic reports on mainly counter of
 * the items in the storage.
 * 
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class OpenAnnotationReportingController extends BaseController {

	def apiKeyAuthenticationService;
	def openAnnotationReportingService;
	def jenaVirtuosoStoreService;
	
	/**
	 * Returns the total of the Annotations
	 */
	def countAnnotations = {
		long startTime = System.currentTimeMillis();
		
		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}

		try {
			int counter = openAnnotationReportingService.countAnnotations(apiKey);
			serializeSimpleResults(startTime, "numberAnnotations", counter);
		} catch(Exception e) {
			log.error("[" + apiKey + "] " + e.getMessage())
			def message = 'Counting of Annotation not completed';
			render(status: 500, text: returnMessage(apiKey, "failure", message, startTime),
				contentType: "text/json", encoding: "UTF-8");
		}
	}
	
	/**
	 * Returns the total of Annotation Sets
	 */
	def countAnnotationSets = {
		long startTime = System.currentTimeMillis();
		
		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}

		try {						
			int counter = openAnnotationReportingService.countAnnotationSets(apiKey);		
			serializeSimpleResults(startTime, "numberAnnotationSets", counter);
		} catch(Exception e) {
			log.error("[" + apiKey + "] " + e.getMessage())
			def message = 'Counting of Annotation Sets not completed';
			render(status: 500, text: returnMessage(apiKey, "failure", message, startTime),
				contentType: "text/json", encoding: "UTF-8");
		}
	}
	
	/**
	 * Returns the total of annotated Resources
	 */
	def countAnnotatedResources = {
		long startTime = System.currentTimeMillis();
		
		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}
		
		try {
			int counter = openAnnotationReportingService.countAnnotatedResources(apiKey);
			serializeSimpleResults(startTime, "numberAnnotatedResources", counter);
		} catch(Exception e) {
			log.error("[" + apiKey + "] " + e.getMessage())
			def message = 'Counting of annotated Resources not completed';
			render(status: 500, text: returnMessage(apiKey, "failure", message, startTime),
				contentType: "text/json", encoding: "UTF-8");
		}
	}
	
	/**
	 * Returns the total of annotated Resources annotated as a whole
	 */
	def countAnnotatedInFullResources = {
		long startTime = System.currentTimeMillis();
		
		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}
		
		try {
			int counter = openAnnotationReportingService.countAnnotatedInFullResources(apiKey);
			serializeSimpleResults(startTime, "numberResourcesAnnotatedInFull", counter);
		} catch(Exception e) {
			log.error("[" + apiKey + "] " + e.getMessage())
			def message = 'Counting of Resources annotated in full not completed';
			render(status: 500, text: returnMessage(apiKey, "failure", message, startTime),
				contentType: "text/json", encoding: "UTF-8");
		}
	}
	
	/**
	 * Returns the total of annotated Resources annotated in their parts
	 */
	def countAnnotatedInPartResources = {
		long startTime = System.currentTimeMillis();
		
		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}
		
		try {
			int counter = openAnnotationReportingService.countAnnotatedInPartResources(apiKey);
			serializeSimpleResults(startTime, "numberResourcesAnnotatedInPart", counter);
		} catch(Exception e) {
			log.error("[" + apiKey + "] " + e.getMessage())
			def message = 'Counting of Resources annotated in part not completed';
			render(status: 500, text: returnMessage(apiKey, "failure", message, startTime),
				contentType: "text/json", encoding: "UTF-8");
		}
	}
	
	/**
	 * Serializes simple on item results (usually counters).
	 * @param startTime		Start time of the task
	 * @param itemLabel		Label of the item
	 * @param item			Item value
	 */
	private void serializeSimpleResults(startTime, itemLabel, item) {
		response.contentType = "text/json;charset=UTF-8";
		response.outputStream << '{"status":"results","duration":"' + (System.currentTimeMillis()-startTime) + '",';
		response.outputStream << '"results":{"' + itemLabel + '":';
		response.outputStream << '"' + item + '"';
		response.outputStream << '}}';
		response.outputStream.flush();
	}
	
	/**
	 * Returns the total of Annotations for each annotated Resource
	 */
	def countAnnotationsForEachResource = {
		long startTime = System.currentTimeMillis();
		
		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}

		try {
			Map map = openAnnotationReportingService.countAnnotationsForAllResources(apiKey);
			serializeMapResults(startTime, map, 'target', 'annotations');
		} catch(Exception e) {
			log.error("[" + apiKey + "] " + e.getMessage())
			def message = 'Counting of Annotations for each Target Resource not completed';
			render(status: 500, text: returnMessage(apiKey, "failure", message, startTime),
				contentType: "text/json", encoding: "UTF-8");
			return;
		}
	}
	
	/**
	 * Return the total of annotations produced by each User
	 */
	def countAnnotationForEachUser = {
		long startTime = System.currentTimeMillis();
		
		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}

		try {
			Map map = openAnnotationReportingService.countAnnotationsForEachUser(apiKey);
			serializeMapResults(startTime, map, 'user', 'annotations');
		} catch(Exception e) {
			log.error("[" + apiKey + "] " + e.getMessage())
			def message = 'Counting of Annotations for each Target Resource not completed';
			render(status: 500, text: returnMessage(apiKey, "failure", message, startTime),
				contentType: "text/json", encoding: "UTF-8");
		}
	}
	
	/**
	 * Return the total of annotations produced by each User
	 */
	def countResourcesAnnotatedByEachUser = {
		long startTime = System.currentTimeMillis();
		
		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}
		
		try {
			Map map = openAnnotationReportingService.countResourcesAnnotatedByEachUser(apiKey);
			serializeMapResults(startTime, map, 'user', 'resources');
		} catch(Exception e) {
			log.error("[" + apiKey + "] " + e.getMessage())
			def message = 'Counting of Annotations for each Target Resource not completed';
			render(status: 500, text: returnMessage(apiKey, "failure", message, startTime),
				contentType: "text/json", encoding: "UTF-8");
		}
	}
	
	/**
	 * Serializes simple on item results (usually grouped counters).
	 * @param startTime		Start time of the task
	 * @param map			The map with the data to serialize
	 * @param keyLabel		Label of the key value
	 * @param valueLabel	Label of the value
	 */
	private void serializeMapResults(startTime, Map map, keyLabel, valueLabel) {
		response.contentType = "text/json;charset=UTF-8";
		response.outputStream << '{"status":"results", "duration":"' + (System.currentTimeMillis()-startTime) + '",';
		response.outputStream << '"results": [';
		Set<String> resources = map.keySet();
		resources.eachWithIndex { resource, i ->
			response.outputStream << '{"' + keyLabel + '":"' + resource + '",';
			response.outputStream << '"' + valueLabel + '":"' + map.get(resource) + '"}';
			if(i<map.size()-1) response.outputStream << ',';
		}
		response.outputStream << ']}';
		response.outputStream.flush();
	}
}
