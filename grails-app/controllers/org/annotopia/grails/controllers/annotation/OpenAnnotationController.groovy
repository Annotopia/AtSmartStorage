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

import javax.servlet.http.HttpServletRequest;

import org.apache.jena.riot.RDFDataMgr

import com.github.jsonldjava.jena.JenaJSONLD
import com.hp.hpl.jena.query.Dataset

/**
 * This is the REST API for managing Open Annotation content.
 * See the current specs here: http://www.openannotation.org/spec/core/
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class OpenAnnotationController {

	def annotationJenaStorageService;
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
			
			int annotationsTotal = annotationJenaStorageService.countAnnotationGraphs(apiKey, tgtUrl, tgtFgt);
			int annotationsPages = (annotationsTotal/Integer.parseInt(max));
			
			if(annotationsTotal>0 && Integer.parseInt(offset)>0 && Integer.parseInt(offset)>=annotationsPages) {
				def message = 'The requested page ' + offset + 
					' does not exist, the page index limit is ' + (annotationsPages==0?"0":(annotationsPages-1));
				render(status: 401, text: returnMessage(apiKey, "rejected", message, startTime), 
					contentType: "text/json", encoding: "UTF-8");
				return;
			}
			
			Set<Dataset> annotationGraphs = annotationJenaStorageService.listAnnotation(apiKey, max, offset, tgtUrl, tgtFgt, tgtExt, tgtIds);
			if(annotationGraphs!=null) {
				response.contentType = "text/json;charset=UTF-8"
				response.outputStream << '{"status":"results", "result": {' +
					'"total":"' + annotationsTotal + '", ' +
					'"pages":"' + annotationsPages + '", ' +
					'"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' +
					'"offset": "' + offset + '", ' +
					'"max": "' + max + '", ' +
					'"items":['
				
				boolean firstStreamed = false // To add the commas between items
				annotationGraphs.each { annotationGraph ->
					if(firstStreamed) response.outputStream << ','
					RDFDataMgr.write(response.outputStream, annotationGraph, JenaJSONLD.JSONLD);
					firstStreamed = true;
				}
				
				response.outputStream <<  ']}}';
				response.outputStream.flush()
			} else {
				// No Annotation Sets found with the specified criteria
				log.info("[" + apiKey + "] No Annotation found with the specified criteria");
				response.contentType = "text/json;charset=UTF-8"
				response.outputStream << '{"status":"nocontent","message":"No results with the chosen criteria" , "result": {' +
					'"total":"' + annotationsTotal + '", ' +
					'"pages":"' + annotationsPages + '", ' +
					'"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' +
					'"offset": "' + offset + '", ' +
					'"max": "' + max + '", ' +
					'"items":['
				response.outputStream <<  ']}}';
				response.outputStream.flush()
			}
		}
		// GET of the annotation identified by the current url
		// Note that the content that is returned might not be the bare Annotation
		// but more likely will be a Named Graph that wraps the annotation.
		// If the annotation is related to multiple graphs, all of the graphs will
		// be returned.
		else {
			Dataset graphs =  annotationJenaStorageService.retrieveAnnotation(apiKey, getCurrentUrl(request));
			
			if(graphs.listNames().hasNext()) {
				response.contentType = "text/json;charset=UTF-8"
				RDFDataMgr.write(response.outputStream, graphs, JenaJSONLD.JSONLD);
				response.outputStream.flush()
			} else {
				// Annotation Set not found
				def message = 'Annotation ' + getCurrentUrl(request) + ' has not been found';
				render(status: 404, text: returnMessage(apiKey, "notfound", message, startTime), contentType: "text/json", encoding: "UTF-8");
				return;
			}
		}
	}
	
	private invalidApiKey(def ip) {
		log.warn("Unauthorized request performed by IP: " + ip)
		def json = JSON.parse('{"status":"rejected" ,"message":"Api Key missing or invalid"}');
		render(status: 401, text: json, contentType: "text/json", encoding: "UTF-8");
	}
	
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
	
	private returnMessage(def apiKey, def status, def message, def startTime) {
		log.info("[" + apiKey + "] " + message);
		return JSON.parse('{"status":"' + status + '","message":"' + message +
			'","duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
	}
}
