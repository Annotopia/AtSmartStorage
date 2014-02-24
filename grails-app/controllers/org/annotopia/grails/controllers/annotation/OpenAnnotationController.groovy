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
package org.annotopia.grails.controllers.annotation

import grails.converters.JSON

import java.text.SimpleDateFormat

import javax.servlet.http.HttpServletRequest

import org.annotopia.groovy.service.store.StoreServiceException
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFLanguages

import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import com.github.jsonldjava.utils.JSONUtils
import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.DatasetFactory

/**
 * This is the REST API for managing Open Annotation content.
 * See the current specs here: http://www.openannotation.org/spec/core/
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class OpenAnnotationController {

	def openAnnotationVirtuosoService;
	def annotationJenaStorageService;
	def openAnnotationStorageService
	def apiKeyAuthenticationService;
	def virtuosoJenaStoreService;
	
	// Date format for all Open Annotation date content
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssz")
	
	/*
	 * GET 
	 * 
	 * Either retrieve a representation of the requested annotation (if an id
	 * is specified and the requested annotation exists) or lists available 
	 * annotations (using pagination).
	 * 
	 * The returned format is compliant with the Open Annotation specification 
	 * http://www.openannotation.org/spec/core/
	 */
	def show = {
		long startTime = System.currentTimeMillis();
		
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}
		
		// GET of a list of annotations
		if(params.id==null) {
			// Pagination
			def max = (request.JSON.max!=null)?request.JSON.max:"10";
			def offset = (request.JSON.offset!=null)?request.JSON.offset:"0";
			
			// Target filters
			def tgtUrl = request.JSON.tgtUrl
			def tgtFgt = request.JSON.tgtFgt
			def tgtExt = request.JSON.tgtExt
			def tgtIds = request.JSON.tgtIds
			def flavor = request.JSON.flavor
			log.info("[" + apiKey + "] List >>" +
				" max:" + max + " offset:" + offset +
				((tgtUrl!=null) ? (" tgtUrl:" + tgtUrl):"") +
				((tgtFgt!=null) ? (" tgtFgt:" + tgtFgt):"") +
				((tgtExt!=null) ? (" tgtExt:" + tgtExt):"") +
				((tgtIds!=null) ? (" tgtIds:" + tgtIds):"") +
				((flavor!=null) ? (" flavor:" + flavor):""));
			
			int annotationsTotal = openAnnotationVirtuosoService.countAnnotationGraphs(apiKey, tgtUrl, tgtFgt);
			int annotationsPages = (annotationsTotal/Integer.parseInt(max));
			
			if(annotationsTotal>0 && Integer.parseInt(offset)>0 && Integer.parseInt(offset)>=annotationsPages) {
				def message = 'The requested page ' + offset + 
					' does not exist, the page index limit is ' + (annotationsPages==0?"0":(annotationsPages-1));
				render(status: 401, text: returnMessage(apiKey, "rejected", message, startTime), 
					contentType: "text/json", encoding: "UTF-8");
				return;
			}
			
			Set<Dataset> annotationGraphs = openAnnotationStorageService.listAnnotation(apiKey, max, offset, tgtUrl, tgtFgt, tgtExt, tgtIds);
			def summaryPrefix = '"total":"' + annotationsTotal + '", ' +
					'"pages":"' + annotationsPages + '", ' +
					'"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' +
					'"offset": "' + offset + '", ' +
					'"max": "' + max + '", ' +
					'"items":[';
					
//     	 	This serializes with and accorting to the context
//			InputStream contextStream = new URL("https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/OAContext.json").openStream();
//			Object contextJson = JSONUtils.fromInputStream(contextStream);

			response.contentType = "text/json;charset=UTF-8"
			if(annotationGraphs!=null) {
				response.outputStream << '{"status":"results", "result": {' + summaryPrefix	
				boolean firstStreamed = false // To add the commas between items
				annotationGraphs.each { annotationGraph ->
					if(firstStreamed) response.outputStream << ','
// This serializes with and accorting to the context
//					ByteArrayOutputStream baos = new ByteArrayOutputStream();
//					RDFDataMgr.write(baos, annotationGraph, RDFLanguages.JSONLD);
//					Object compact = JsonLdProcessor.compact(JSONUtils.fromString(baos.toString()), contextJson,  new JsonLdOptions());
//					response.outputStream << JSONUtils.toPrettyString(compact)
					RDFDataMgr.write(response.outputStream, annotationGraph, RDFLanguages.JSONLD);
					firstStreamed = true;
				}
			} else {
				// No Annotation Sets found with the specified criteria
				log.info("[" + apiKey + "] No Annotation found with the specified criteria");			
				response.outputStream << '{"status":"nocontent","message":"No results with the chosen criteria" , "result": {' + summaryPrefix
			}
			response.outputStream <<  ']}}';
			response.outputStream.flush()
		}
		// GET of the annotation identified by the current url
		// Note that the content that is returned might not be the bare Annotation
		// but more likely will be a Named Graph that wraps the annotation.
		// If the annotation is related to multiple graphs, all of the graphs will
		// be returned.
		else {
			Dataset graphs =  openAnnotationVirtuosoService.retrieveAnnotation(apiKey, getCurrentUrl(request));
			
			if(graphs.listNames().hasNext()) {
				response.contentType = "text/json;charset=UTF-8"
				RDFDataMgr.write(response.outputStream, graphs, RDFLanguages.JSONLD);
				response.outputStream.flush()
			} else {
				// Annotation Set not found
				def message = 'Annotation ' + getCurrentUrl(request) + ' has not been found';
				render(status: 404, text: returnMessage(apiKey, "notfound", message, startTime), contentType: "text/json", encoding: "UTF-8");
				return;
			}
		}
	}
	
	/*
	 * POST
	 *
	 * It accepts an annotation item formatted according to the Open Annotation specification
	 * http://www.openannotation.org/spec/core/
	 * 
	 * The single annotation can be wrapped in a graph or not. This is not currently handling 
	 * multiple annotations.
	 * 
	 * Validation not yet implemented.
	 */
	def save = {
		long startTime = System.currentTimeMillis();
		
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}
		
		def item = request.JSON.item
		def flavor = (request.JSON.flavor!=null)?request.JSON.flavor:"OA";
		def validate = (request.JSON.validate!=null)?request.JSON.validate:"OFF";
				
		if(item!=null) {
			log.warn("[" + apiKey + "] TODO: Validation of the Annotation content requested but not implemented yest!");
			
			
			Dataset inMemoryDataset = DatasetFactory.createMem();
			try {
				RDFDataMgr.read(inMemoryDataset, new ByteArrayInputStream(item.toString().getBytes("UTF-8")), RDFLanguages.JSONLD);
			} catch (Exception ex) {
				log.error(ex.getMessage());
				def message = "Annotation cannot be read";
				render(status: 500, text: returnMessage(apiKey, "invalidcontent", message, startTime), contentType: "text/json", encoding: "UTF-8");
				return;
			}
			
			Dataset savedAnnotation;
			try {
				savedAnnotation = openAnnotationStorageService.saveAnnotationDataset(apiKey, startTime, inMemoryDataset);
			} catch(StoreServiceException exception) {
				render(status: exception.status, text: exception.text, contentType: exception.contentType, encoding: exception.encoding);
				return;
			}
			
			if(savedAnnotation!=null) { 
				response.contentType = "text/json;charset=UTF-8"
				response.outputStream << '{"status":"saved", "result": {' +
					'"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' +
					'"item":['
						
				RDFDataMgr.write(response.outputStream, savedAnnotation, RDFLanguages.JSONLD);
				
				response.outputStream <<  ']}}';
				response.outputStream.flush()
			} else {
				// Dataset returned null
				def message = "Null Dataset. Something went terribly wrong";
				render(status: 500, text: returnMessage(apiKey, "exception", message, startTime), contentType: "text/json", encoding: "UTF-8");
			}
		} else {
			// Annotation Set not found
			def message = "No annotation found in the request";
			render(status: 200, text: returnMessage(apiKey, "nocontent", message, startTime), contentType: "text/json", encoding: "UTF-8");
		}
	}
	
	/*
	 * PUT
	 *
	 * It accepts updates existing annotation item formatted according to the Open Annotation specification
	 * http://www.openannotation.org/spec/core/
	 *
	 * The single annotation can be wrapped in a graph or not. This is not currently handling
	 * multiple annotations.
	 *
	 * Validation not yet implemented.
	 */
	def update = {
		long startTime = System.currentTimeMillis();
		
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}
		
		def item = request.JSON.item
		def flavor = (request.JSON.flavor!=null)?request.JSON.flavor:"OA";
		def validate = (request.JSON.validate!=null)?request.JSON.validate:"OFF";
				
		if(item!=null) {
			log.warn("[" + apiKey + "] TODO: Validation of the Annotation content requested but not implemented yest!");
			
			Dataset inMemoryDataset = DatasetFactory.createMem();
			try {
				RDFDataMgr.read(inMemoryDataset, new ByteArrayInputStream(item.toString().getBytes("UTF-8")), RDFLanguages.JSONLD);
			} catch (Exception ex) {
				def message = "Annotation cannot be read";
				render(status: 500, text: returnMessage(apiKey, "invalidcontent", message, startTime), contentType: "text/json", encoding: "UTF-8");
				return;
			}
			
			Dataset updatedAnnotation;
			try {
				updatedAnnotation = openAnnotationStorageService.updateAnnotationDataset(apiKey, startTime, inMemoryDataset);
			} catch(StoreServiceException exception) {
				render(status: exception.status, text: exception.text, contentType: exception.contentType, encoding: exception.encoding);
				return;
			}
			
			if(updatedAnnotation!=null) {
				response.contentType = "text/json;charset=UTF-8"
				response.outputStream << '{"status":"updated", "result": {' +
					'"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' +
					'"item":['
						
				RDFDataMgr.write(response.outputStream, updatedAnnotation, RDFLanguages.JSONLD);
				
				response.outputStream <<  ']}}';
				response.outputStream.flush()
			} else {
				// Dataset returned null
				def message = "Null Dataset. Something went terribly wrong";
				render(status: 500, text: returnMessage(apiKey, "exception", message, startTime), contentType: "text/json", encoding: "UTF-8");
			}
		} else {
			// Annotation Set not found
			def message = "No annotation found in the request";
			render(status: 200, text: returnMessage(apiKey, "nocontent", message, startTime), contentType: "text/json", encoding: "UTF-8");
		}
	}
	
	/**
	 * Logging and message for invalid API key.
	 * @param ip	Ip of the client that issued the request
	 */
	private void invalidApiKey(def ip) {
		log.warn("Unauthorized request performed by IP: " + ip)
		def json = JSON.parse('{"status":"rejected" ,"message":"Api Key missing or invalid"}');
		render(status: 401, text: json, contentType: "text/json", encoding: "UTF-8");
	}
	
	/**
	 * Returns the current URL.
	 * @param request 	The HTTP request
	 * @return	The current URL
	 */
	static String getCurrentUrl(HttpServletRequest request){
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
	private returnMessage(def apiKey, def status, def message, def startTime) {
		log.info("[" + apiKey + "] " + message);
		return JSON.parse('{"status":"' + status + '","message":"' + message +
			'","duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
	}
}
