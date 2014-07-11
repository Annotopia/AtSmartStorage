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

import javax.servlet.http.HttpServletResponse

import org.annotopia.groovy.service.store.BaseController
import org.annotopia.groovy.service.store.StoreServiceException
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFLanguages
import org.codehaus.groovy.grails.web.json.JSONObject

import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import com.github.jsonldjava.utils.JsonUtils
import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.DatasetFactory
import com.hp.hpl.jena.rdf.model.Model

/**
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class OpenAnnotationSetController extends BaseController {

//	String AT_CONTEXT = "https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/AnnotopiaContext.json";
//	String AT_FRAME = "https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/AnnotopiaFrame.json";
	
	def grailsApplication;
	def jenaVirtuosoStoreService;
	def apiKeyAuthenticationService;
	def openAnnotationSetStorageService;
	def openAnnotationVirtuosoService;
	def openAnnotationSetVirtuosoService;

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
		
		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(apiKey==null) apiKey = params.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}
		
		// Response format parametrization and constraints
		def outCmd = (request.JSON.outCmd!=null)?request.JSON.outCmd:"none";
		if(params.outCmd!=null) outCmd = params.outCmd;		
		def incGph = (request.JSON.incGph!=null)?request.JSON.incGph:"false";
		if(params.incGph!=null) incGph = params.incGph;
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
			if(params.max!=null) max = params.max;
			def offset = (request.JSON.offset!=null)?request.JSON.offset:"0";
			if(params.offset!=null) offset = params.offset;
			
			// Target filters
			def tgtUrl = request.JSON.tgtUrl
			if(params.tgtUrl!=null) tgtUrl = params.tgtUrl;
			def tgtFgt = (request.JSON.tgtFgt!=null)?request.JSON.tgtFgt:"true"; 
			if(params.tgtFgt!=null) tgtFgt = params.tgtFgt;
			
			// Target IDs
			Map<String,String> identifiers = new HashMap<String,String>();
			def tgtIds = (request.JSON.tgtIds!=null)?request.JSON.tgtIds:null;
			if(params.tgtIds!=null) tgtIds = params.tgtIds
			if(tgtIds!=null) {
				JSONObject ids = JSON.parse(tgtIds);
				ids.keys().each { key ->
					identifiers.put(key, ids.get(key));
				}
			}
			
			// Currently unusued, planned
			def tgtExt = request.JSON.tgtExt
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
			
			List<String> tgtUrls ;
			if(tgtUrl!=null) {
				tgtUrls = new ArrayList<String>();
				tgtUrls.add(tgtUrl);
			} else if(tgtIds!=null) {
				tgtUrls = new ArrayList<String>();
				tgtUrls = jenaVirtuosoStoreService.retrieveAllManifestationsByIdentifiers(apiKey, identifiers, grailsApplication.config.annotopia.storage.uri.graph.identifiers);
			}
			
			int annotationSetsTotal = openAnnotationSetVirtuosoService.countAnnotationSetGraphs(apiKey, tgtUrls, tgtFgt);
			int annotationSetsPages = (annotationSetsTotal/Integer.parseInt(max));
			if(annotationSetsTotal>0 && Integer.parseInt(offset)>0 && Integer.parseInt(offset)>=annotationSetsPages) {
				def message = 'The requested page ' + offset +
					' does not exist, the page index limit is ' + (annotationSetsPages==0?"0":(annotationSetsPages-1));
				render(status: 401, text: returnMessage(apiKey, "rejected", message, startTime),
					contentType: "text/json", encoding: "UTF-8");
				return;
			}
			
			Set<Dataset> annotationSets = openAnnotationSetStorageService.listAnnotationSets(apiKey, max, offset, tgtUrl, tgtFgt, tgtExt, tgtIds, incGph);
			def summaryPrefix = '"total":"' + annotationSetsTotal + '", ' +
					'"pages":"' + annotationSetsPages + '", ' +
					'"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' +
					'"offset": "' + offset + '", ' +
					'"max": "' + max + '", ' +
					'"sets":[';
					
			Object contextJson = null;
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
								contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey, grailsApplication.config.annotopia.jsonld.annotopia.context));
							} else if(outCmd=='frame') {
								contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey, grailsApplication.config.annotopia.jsonld.annotopia.framing));
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
							Object compact = JsonLdProcessor.compact(JsonUtils.fromString(baos.toString()), contextJson,  new JsonLdOptions());
							response.outputStream << JsonUtils.toPrettyString(compact)
						}  else if(outCmd=='frame') {
							Object framed =  JsonLdProcessor.frame(JsonUtils.fromString(baos.toString()),contextJson, new JsonLdOptions());
							response.outputStream << JsonUtils.toPrettyString(framed)
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
			
			Dataset graphs =  openAnnotationSetVirtuosoService.retrieveAnnotationSet(apiKey, url);
			
			Object contextJson = null;
			if(graphs!=null && graphs.listNames().hasNext()) {
				// Enabling CORS
				//response.setHeader('Access-Control-Allow-Origin', '*')
				//response.contentType = "application/json;charset=UTF-8"
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
							contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey, grailsApplication.config.annotopia.jsonld.annotopia.context));
						} else if(outCmd=='frame') {
							contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey, grailsApplication.config.annotopia.jsonld.annotopia.framing));
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
						Object compact = JsonLdProcessor.compact(JsonUtils.fromString(baos.toString()), contextJson,  new JsonLdOptions());
						response.outputStream << JsonUtils.toPrettyString(compact)
					}  else if(outCmd=='frame') {
						Object framed =  JsonLdProcessor.frame(JsonUtils.fromString(baos.toString()),contextJson, new JsonLdOptions());
						response.outputStream << JsonUtils.toPrettyString(framed)
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
		
		// Response format parametrization and constraints
		def outCmd = (request.JSON.outCmd!=null)?request.JSON.outCmd:"none";
		if(params.outCmd!=null) outCmd = params.outCmd;
		def incGph = (request.JSON.incGph!=null)?request.JSON.incGph:"false";
		if(params.incGph!=null) incGph = params.incGph;
		if(outCmd=='frame' && incGph=='true') {
			log.warn("[" + apiKey + "] Invalid options, framing does not currently support Named Graphs");
			def message = 'Invalid options, framing does not currently support Named Graphs';
			render(status: 401, text: returnMessage(apiKey, "rejected", message, startTime),
				contentType: "text/json", encoding: "UTF-8");
			return;
		}
		
		log.info("[" + apiKey + "] Saving Annotation Set");
		
		// Parsing the incoming parameters
		def set = request.JSON.set
		
		// Unused but planned
		def flavor = (request.JSON.flavor!=null)?request.JSON.flavor:"OA";
		def validate = (request.JSON.validate!=null)?request.JSON.validate:"OFF";
				
		if(set!=null) {	
			Dataset savedAnnotationSet;
			try {
				savedAnnotationSet = openAnnotationSetStorageService.saveAnnotationSet(apiKey, startTime, Boolean.parseBoolean(incGph), set.toString()); 
			} catch(StoreServiceException exception) {
				render(status: exception.status, text: exception.text, contentType: exception.contentType, encoding: exception.encoding);
				return;
			}
			
			if(savedAnnotationSet!=null) {
				
				renderSavedNamedGraphsDataset(apiKey, startTime, outCmd, 'saved', response, savedAnnotationSet);
				
//				Object contextJson = null;
//				if(outCmd=='none') {
//					if(incGph=='false') {
//						Model m = savedAnnotationSet.getNamedModel(savedAnnotationSet.listNames().next());
//						RDFDataMgr.write(response.outputStream, m, RDFLanguages.JSONLD);
//					} else {
//						RDFDataMgr.write(response.outputStream, savedAnnotationSet, RDFLanguages.JSONLD);
//					}
//				} else {
//					if(contextJson==null) {
//						if(outCmd=='context') {
//							contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey, grailsApplication.config.annotopia.jsonld.annotopia.context));
//						} else if(outCmd=='frame') {
//							contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey, grailsApplication.config.annotopia.jsonld.annotopia.framing));
//						}
//					}
//				
//					ByteArrayOutputStream baos = new ByteArrayOutputStream();
//					if(incGph=='false') {
//						Model m = savedAnnotationSet.getNamedModel(savedAnnotationSet.listNames().next());
//						RDFDataMgr.write(baos, m.getGraph(), RDFLanguages.JSONLD);
//					} else {
//						RDFDataMgr.write(baos, savedAnnotationSet, RDFLanguages.JSONLD);
//					}
//					
//					if(outCmd=='context') {
//						Object compact = JsonLdProcessor.compact(JsonUtils.fromString(baos.toString()), contextJson, new JsonLdOptions());
//						response.outputStream << JsonUtils.toPrettyString(compact)
//					}  else if(outCmd=='frame') {
//						Object framed =  JsonLdProcessor.frame(JsonUtils.fromString(baos.toString()), contextJson, new JsonLdOptions());
//						response.outputStream << JsonUtils.toPrettyString(framed)
//					}
//				}
//				response.outputStream.flush()
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
		
		// Response format parametrization and constraints
		def outCmd = (request.JSON.outCmd!=null)?request.JSON.outCmd:"none";
		if(params.outCmd!=null) outCmd = params.outCmd;
		def incGph = (request.JSON.incGph!=null)?request.JSON.incGph:"false";
		if(params.incGph!=null) incGph = params.incGph;
		if(outCmd=='frame' && incGph=='true') {
			log.warn("[" + apiKey + "] Invalid options, framing does not currently support Named Graphs");
			def message = 'Invalid options, framing does not currently support Named Graphs';
			render(status: 401, text: returnMessage(apiKey, "rejected", message, startTime),
				contentType: "text/json", encoding: "UTF-8");
			return;
		}
		
		log.info("[" + apiKey + "] Updating Annotation Set");
		
		// Parsing the incoming parameters
		def set = request.JSON.set
		
		// Unused but planned
		def flavor = (request.JSON.flavor!=null)?request.JSON.flavor:"OA";
		def validate = (request.JSON.validate!=null)?request.JSON.validate:"OFF";
				
		if(set!=null) {	
			Dataset updatedAnnotationSet;
			try {
				println set.toString();
				updatedAnnotationSet = openAnnotationSetStorageService.updateAnnotationSet(apiKey, startTime, Boolean.parseBoolean(incGph), set.toString()); 
			} catch(StoreServiceException exception) {
				render(status: exception.status, text: exception.text, contentType: exception.contentType, encoding: exception.encoding);
				return;
			}
			
			if(updatedAnnotationSet!=null) {
				
//				ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//				RDFDataMgr.write(outputStream, updatedAnnotationSet, RDFLanguages.JSONLD);
//				println outputStream.toString();
//				
				renderSavedNamedGraphsDataset(apiKey, startTime, outCmd, 'updated', response, updatedAnnotationSet);
				
//				if(outCmd=='none') {
//					if(incGph=='false') {
//						Model m = updatedAnnotationSet.getNamedModel(updatedAnnotationSet.listNames().next());
//						RDFDataMgr.write(response.outputStream, m, RDFLanguages.JSONLD);
//					} else {
//						RDFDataMgr.write(response.outputStream, updatedAnnotationSet, RDFLanguages.JSONLD);
//					}
//				} else {
//					if(contextJson==null) {
//						if(outCmd=='context') {
//							contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey, grailsApplication.config.annotopia.jsonld.annotopia.context));
//						} else if(outCmd=='frame') {
//							contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey, grailsApplication.config.annotopia.jsonld.annotopia.framing));
//						}
//					}
//				
//					ByteArrayOutputStream baos = new ByteArrayOutputStream();
//					if(incGph=='false') {
//						Model m = updatedAnnotationSet.getNamedModel(updatedAnnotationSet.listNames().next());
//						RDFDataMgr.write(baos, m.getGraph(), RDFLanguages.JSONLD);
//					} else {
//						RDFDataMgr.write(baos, updatedAnnotationSet, RDFLanguages.JSONLD);
//					}
//					
//					if(outCmd=='context') {
//						Object compact = JsonLdProcessor.compact(JsonUtils.fromString(baos.toString()), contextJson,  new JsonLdOptions());
//						response.outputStream << JsonUtils.toPrettyString(compact)
//					}  else if(outCmd=='frame') {
//						Object framed =  JsonLdProcessor.frame(JsonUtils.fromString(baos.toString()), contextJson, new JsonLdOptions());
//						response.outputStream << JsonUtils.toPrettyString(framed)
//					}
//				}
//				response.outputStream.flush()
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
	
	private void renderSavedNamedGraphsDataset(def apiKey, long startTime, String outCmd, String status, HttpServletResponse response, Dataset dataset) {
		response.contentType = "text/json;charset=UTF-8"
		
		// Count graphs
		int sizeDataset = 0;
		Iterator iterator = dataset.listNames();
		while(iterator.hasNext()) {
			sizeDataset++;
			iterator.next();
		}
		
		if(sizeDataset>1 && outCmd=='frame') {
			log.warn("[" + apiKey + "] Invalid options, framing does not currently support Named Graphs");
			def message = 'Invalid options, framing does not currently support Named Graphs';
			render(status: 401, text: returnMessage(apiKey, "rejected", message, startTime),
				contentType: "text/json", encoding: "UTF-8");
			return;
		}
		
		Dataset datasetToRender = DatasetFactory.createMem();
		if(sizeDataset==1) datasetToRender.setDefaultModel(dataset.getNamedModel(dataset.listNames().next()));
		else datasetToRender = dataset;
		
		def summaryPrefix = '"duration": "' + (System.currentTimeMillis()-startTime) + 'ms","graphs":"' + sizeDataset +  '",' + '"set":[';
		response.outputStream << '{"status":"' + status + '", "result": {' + summaryPrefix
		
		Object contextJson = null;
		if(outCmd=='none') {
			RDFDataMgr.write(response.outputStream, datasetToRender, RDFLanguages.JSONLD);
		} else {
			if(contextJson==null) {
				if(outCmd=='context') {
					contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey, grailsApplication.config.annotopia.jsonld.annotopia.context));
				} else if(sizeDataset==1 && outCmd=='frame') {
					contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey,grailsApplication.config.annotopia.jsonld.annotopia.framing));
				} 
			}
		
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			RDFDataMgr.write(baos, datasetToRender, RDFLanguages.JSONLD);
			
			if(outCmd=='context') {
				Object compact = JsonLdProcessor.compact(JsonUtils.fromString(baos.toString()), contextJson, new JsonLdOptions());
				response.outputStream << JsonUtils.toPrettyString(compact)
			}  else if(sizeDataset==1 && outCmd=='frame') {
				Object framed =  JsonLdProcessor.frame(JsonUtils.fromString(baos.toString()), contextJson, new JsonLdOptions());
				response.outputStream << JsonUtils.toPrettyString(framed)
			}
		}
		response.outputStream << ']}}'
		response.outputStream.flush()
	}
}
