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

import grails.converters.JSON

import java.text.SimpleDateFormat

import javax.servlet.http.HttpServletRequest

import org.annotopia.groovy.service.store.BaseController;
import org.annotopia.groovy.service.store.StoreServiceException
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFLanguages

import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import com.github.jsonldjava.utils.JSONUtils
import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.rdf.model.Model

/**
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class OpenAnnotationSetController extends BaseController {

	def apiKeyAuthenticationService;
	def openAnnotationStorageService;
	def openAnnotationVirtuosoService;

	/*
	 * GET
	 *
	 * Either retrieve a representation of the requested annotation set (if an 
	 * id is specified and the requested annotation set exists) or lists 
	 * available annotation sets (using pagination).
	 *
	 * The returned format is compliant with the Open Annotation specification
	 * http://www.openannotation.org/spec/core/
	 */
	def show = {
		long startTime = System.currentTimeMillis();
		
		def apiKey = request.JSON.apiKey;
		if(apiKey==null) apiKey = params.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}
		
		def outCmd = (request.JSON.outCmd!=null)?request.JSON.outCmd:"none";
		if(params.outCmd!=null) outCmd = params.outCmd;
		
		def incGph = (request.JSON.incGph!=null)?request.JSON.incGph:"false";
		
		if(outCmd=='frame' && incGph=='true') {
			log.warn("[" + apiKey + "] Invalid options, framing does not currently support Named Graphs");
			def message = 'Invalid options, framing does not currently support Named Graphs';
			render(status: 401, text: returnMessage(apiKey, "rejected", message, startTime),
				contentType: "text/json", encoding: "UTF-8");
			return;
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
				((flavor!=null) ? (" flavor:" + flavor):"") +
				((outCmd!=null) ? (" outCmd:" + outCmd):"") +
				((incGph!=null) ? (" incGph:" + incGph):""));
			
			int annotationSetsTotal = openAnnotationVirtuosoService.countAnnotationSetGraphs(apiKey, tgtUrl, tgtFgt);
			int annotationSetsPages = (annotationSetsTotal/Integer.parseInt(max));
			if(annotationSetsTotal>0 && Integer.parseInt(offset)>0 && Integer.parseInt(offset)>=annotationSetsPages) {
				def message = 'The requested page ' + offset +
					' does not exist, the page index limit is ' + (annotationSetsPages==0?"0":(annotationSetsPages-1));
				render(status: 401, text: returnMessage(apiKey, "rejected", message, startTime),
					contentType: "text/json", encoding: "UTF-8");
				return;
			}
			
			Set<Dataset> annotationSets = openAnnotationStorageService.listAnnotationSets(apiKey, max, offset, tgtUrl, tgtFgt, tgtExt, tgtIds, incGph);
			def summaryPrefix = '"total":"' + annotationSetsTotal + '", ' +
					'"pages":"' + annotationSetsPages + '", ' +
					'"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' +
					'"offset": "' + offset + '", ' +
					'"max": "' + max + '", ' +
					'"sets":[';
					
			Object contextJson = null;
			// Enabling CORS
			response.setHeader('Access-Control-Allow-Origin', '*')
			response.contentType = "application/json;charset=UTF-8"
			
			if(annotationSets!=null) {
				response.outputStream << '{"status":"results", "result": {' + summaryPrefix
				boolean firstStreamed = false // To add the commas between items
				annotationSets.each { annotationSet ->
					if(firstStreamed) response.outputStream << ','
					if(outCmd=='none') {
						if(incGph=='false') {
							if(annotationSet.listNames().hasNext()) {
								Model m = annotationSet.getNamedModel(annotationSet.listNames().next());
								RDFDataMgr.write(response.outputStream, m.getGraph(), RDFLanguages.JSONLD);
							}
						} else {
							RDFDataMgr.write(response.outputStream, annotationSet, RDFLanguages.JSONLD);
						}
					} else {
						// This serializes with and according to the context
						if(contextJson==null) {
							if(outCmd=='context') {
								contextJson = JSONUtils.fromInputStream(new URL("https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/AnnotopiaContext.json").openStream());
							} else if(outCmd=='frame') {
								contextJson = JSONUtils.fromInputStream(new URL("https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/AnnotopiaFrame.json").openStream());
							}
						}

						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						if(incGph=='false') {
							if(annotationSet.listNames().hasNext()) {
								Model m = annotationSet.getNamedModel(annotationSet.listNames().next());
								RDFDataMgr.write(baos, m.getGraph(), RDFLanguages.JSONLD);
							}
						} else {
							RDFDataMgr.write(baos, annotationSet, RDFLanguages.JSONLD);
						}
						
						if(outCmd=='context') {
							Object compact = JsonLdProcessor.compact(JSONUtils.fromString(baos.toString()), contextJson,  new JsonLdOptions());
							response.outputStream << JSONUtils.toPrettyString(compact)
						}  else if(outCmd=='frame') {
							Object framed =  JsonLdProcessor.frame(JSONUtils.fromString(baos.toString()),contextJson, new JsonLdOptions());
							response.outputStream << JSONUtils.toPrettyString(framed)
						}
					}
					firstStreamed = true;
				}
			} else {
				// No Annotation Sets found with the specified criteria
				log.info("[" + apiKey + "] No Annotation sets found with the specified criteria");			
				response.outputStream << '{"status":"nocontent","message":"No results with the chosen criteria" , "result": {' + summaryPrefix
			}
			
					
			response.outputStream <<  ']}}';
			response.outputStream.flush()
		}
		// GET of the annotation set identified by the current url
		// Note that the content that is returned might not be the bare Annotation
		// but more likely will be a Named Graph that wraps the annotation.
		// If the annotation is related to multiple graphs, all of the graphs will
		// be returned.
		else {
			String url = getCurrentUrl(request).substring(0, getCurrentUrl(request).indexOf("?"))
			
			Dataset graphs =  openAnnotationVirtuosoService.retrieveAnnotationSet(apiKey, url);
			
			Object contextJson = null;
			if(graphs!=null && graphs.listNames().hasNext()) {
				// Enabling CORS
				response.setHeader('Access-Control-Allow-Origin', '*')
				response.contentType = "application/json;charset=UTF-8"
				if(outCmd=='none') {
					if(incGph=='false') {
						Model m = graphs.getNamedModel(graphs.listNames().next());
						RDFDataMgr.write(response.outputStream, m, RDFLanguages.JSONLD);
					} else {
						RDFDataMgr.write(response.outputStream, graphs, RDFLanguages.JSONLD);
					}
				} else {
					if(contextJson==null) {
						if(outCmd=='context') {
							contextJson = JSONUtils.fromInputStream(new URL("https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/AnnotopiaContext.json").openStream());
						} else if(outCmd=='frame') {
							contextJson = JSONUtils.fromInputStream(new URL("https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/AnnotopiaFrame.json").openStream());
						}
					}
				
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					if(incGph=='false') {
						Model m = graphs.getNamedModel(graphs.listNames().next());
						RDFDataMgr.write(baos, m.getGraph(), RDFLanguages.JSONLD);
					} else {
						RDFDataMgr.write(baos, graphs, RDFLanguages.JSONLD);
					}
					
					if(outCmd=='context') {
						Object compact = JsonLdProcessor.compact(JSONUtils.fromString(baos.toString()), contextJson,  new JsonLdOptions());
						response.outputStream << JSONUtils.toPrettyString(compact)
					}  else if(outCmd=='frame') {
						Object framed =  JsonLdProcessor.frame(JSONUtils.fromString(baos.toString()),contextJson, new JsonLdOptions());
						response.outputStream << JSONUtils.toPrettyString(framed)
					}
				}
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
	 * It accepts an annotation set formatted according to the Open Annotation specification
	 * http://www.openannotation.org/spec/core/
	 *
	 * The single annotation set can be wrapped in a graph or not. This is not currently 
	 * handling multiple annotations.
	 *
	 * Validation not yet implemented.
	 */
	def save = {
		long startTime = System.currentTimeMillis();
		
		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}
		
		log.info("[" + apiKey + "] Saving Annotation Set");
		
		// Parsing the incoming parameters
		def set = request.JSON.set
		def flavor = (request.JSON.flavor!=null)?request.JSON.flavor:"OA";
		def validate = (request.JSON.validate!=null)?request.JSON.validate:"OFF";
				
		if(set!=null) {	
			Dataset savedAnnotationSet;
			try {
				savedAnnotationSet = openAnnotationStorageService.saveAnnotationSet(apiKey, startTime, set.toString()); 
			} catch(StoreServiceException exception) {
				render(status: exception.status, text: exception.text, contentType: exception.contentType, encoding: exception.encoding);
				return;
			}
			
			if(savedAnnotationSet!=null) {
				// Enable CORS
				response.setHeader('Access-Control-Allow-Origin', '*')
				// Streams back the saved annotation with the proper provenance
				response.contentType = "text/json;charset=UTF-8"
				response.outputStream << '{"status":"saved", "result": {' +
					'"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' +
					'"item":['
						
				RDFDataMgr.write(response.outputStream, savedAnnotationSet, RDFLanguages.JSONLD);
				
				response.outputStream <<  ']}}';
				response.outputStream.flush()
			} else {
				// Dataset returned null
				def message = "Null Annotation Set Dataset. Something went terribly wrong";
				render(status: 500, text: returnMessage(apiKey, "exception", message, startTime), contentType: "text/json", encoding: "UTF-8");
			}
		} else {
			// Annotation not found
			def message = "No annotation set found in the request";
			render(status: 200, text: returnMessage(apiKey, "nocontent", message, startTime), contentType: "text/json", encoding: "UTF-8");
		}
	}
	
	/*
	 * PUT
	 *
	 * It accepts updates existing annotation set formatted according to the Open Annotation specification
	 * http://www.openannotation.org/spec/core/
	 *
	 * The single annotation set can be wrapped in a graph or not. 
	 *
	 * Validation not yet implemented.
	 */
	def update = {
		long startTime = System.currentTimeMillis();
		
		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}
		
		def set = request.JSON.set
		def flavor = (request.JSON.flavor!=null)?request.JSON.flavor:"OA";
		def validate = (request.JSON.validate!=null)?request.JSON.validate:"OFF";
				
		if(set!=null) {
			//Dataset savedDataset = openAnnotationStorageService.saveAnnotationSet(apiKey, startTime, set.toString());
		} else {
			// Annotation Set not found
			def message = "No annotation set found in the request";
			render(status: 200, text: returnMessage(apiKey, "nocontent", message, startTime), contentType: "text/json", encoding: "UTF-8");
		}
	}
}
