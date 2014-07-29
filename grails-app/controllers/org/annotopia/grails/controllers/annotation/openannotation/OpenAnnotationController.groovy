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
import org.codehaus.groovy.grails.web.json.JSONArray
import org.codehaus.groovy.grails.web.json.JSONObject

import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import com.github.jsonldjava.utils.JsonUtils
import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.DatasetFactory
import com.hp.hpl.jena.rdf.model.Model

/**
 * This is the REST API for managing Open Annotation content.
 * See the current specs here: http://www.openannotation.org/spec/core/
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class OpenAnnotationController extends BaseController {

	//String OA_CONTEXT = "https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/OAContext.json";
	//String OA_FRAME = "https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/OAFrame.json";
	
	private final RESPONSE_CONTENT_TYPE = "application/json;charset=UTF-8";
	
	// outCmd (output command) constants
	private final OUTCMD_NONE = "none";
	private final OUTCMD_FRAME = "frame";
	private final OUTCMD_CONTEXT = "context";
	
	// incGph (Include graph) constants
	private final INCGPH_YES = "true";
	private final INCGPH_NO = "false";
	
	def openAnnotationVirtuosoService;
	def annotationJenaStorageService;
	def openAnnotationStorageService
	def openAnnotationValidationService;
	def apiKeyAuthenticationService;
	def jenaVirtuosoStoreService;

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
		if(apiKey==null) apiKey = params.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}
		
		// Response format parametrization and constraints
		def outCmd = (request.JSON.outCmd!=null)?request.JSON.outCmd:OUTCMD_NONE;
		if(params.outCmd!=null) outCmd = params.outCmd;
		def incGph = (request.JSON.incGph!=null)?request.JSON.incGph:INCGPH_NO;
		if(params.incGph!=null) incGph = params.incGph;
		if(outCmd==OUTCMD_FRAME && incGph==INCGPH_YES) {
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
			
			// Facets
			def sources = request.JSON.sources
			if(params.sources!=null) sources = params.sources;
			def sourcesFacet = []
			if(sources) sourcesFacet = sources.split(",");
			def motivations = request.JSON.motivations
			if(params.motivations!=null) motivations = params.motivations;
			def motivationsFacet = []
			if(motivations) motivationsFacet = motivations.split(",");
			
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
			
			int annotationsTotal = openAnnotationVirtuosoService.countAnnotationGraphs(apiKey, tgtUrls, tgtFgt, sourcesFacet, motivationsFacet);			
			//int annotationsTotal = openAnnotationVirtuosoService.countAnnotationGraphs(apiKey, tgtUrl, tgtFgt, identifiers);
			int annotationsPages = (annotationsTotal/Integer.parseInt(max));		
			if(annotationsTotal>0 && Integer.parseInt(offset)>0 && Integer.parseInt(offset)>=annotationsPages) {
				def message = 'The requested page ' + offset + 
					' does not exist, the page index limit is ' + (annotationsPages==0?"0":(annotationsPages-1));
				render(status: 401, text: returnMessage(apiKey, "rejected", message, startTime), 
					contentType: "text/json", encoding: "UTF-8");
				return;
			}
			
			Set<Dataset> annotationGraphs = openAnnotationStorageService.listAnnotation(apiKey, max, offset, tgtUrls, tgtFgt, tgtExt, tgtIds, incGph, sourcesFacet, motivationsFacet);
			def summaryPrefix = '"total":"' + annotationsTotal + '", ' +
					'"pages":"' + annotationsPages + '", ' +
					'"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' +
					'"offset": "' + offset + '", ' +
					'"max": "' + max + '", ' +
					'"items":[';
					
			Object contextJson = null;
			response.contentType = RESPONSE_CONTENT_TYPE	
			if(annotationGraphs!=null) {
				response.outputStream << '{"status":"results", "result": {' + summaryPrefix	
				boolean firstStreamed = false // To add the commas between items
				annotationGraphs.each { annotationGraph ->
					if(firstStreamed) response.outputStream << ','
					if(outCmd==OUTCMD_NONE) {
						if(incGph==INCGPH_NO) {
							if(annotationGraph.listNames().hasNext()) {
								Model m = annotationGraph.getNamedModel(annotationGraph.listNames().next());
								RDFDataMgr.write(response.outputStream, m.getGraph(), RDFLanguages.JSONLD);
							}
						} else {
							RDFDataMgr.write(response.outputStream, annotationGraph, RDFLanguages.JSONLD);
						}
					} else {
						// This serializes with and according to the context
						if(contextJson==null) {
							if(outCmd==OUTCMD_CONTEXT) {
								contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey, grailsApplication.config.annotopia.jsonld.openannotation.context));
							} else if(outCmd==OUTCMD_FRAME) {
								contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey, grailsApplication.config.annotopia.jsonld.openannotation.framing));						
							}
						}

						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						if(incGph==INCGPH_NO) {
							if(annotationGraph.listNames().hasNext()) {
								Model m = annotationGraph.getNamedModel(annotationGraph.listNames().next());
								RDFDataMgr.write(baos, m.getGraph(), RDFLanguages.JSONLD);
							}
						} else {
							RDFDataMgr.write(baos, annotationGraph, RDFLanguages.JSONLD);
						}
						
						if(outCmd==OUTCMD_CONTEXT) {
							Object compact = JsonLdProcessor.compact(JsonUtils.fromString(baos.toString()), contextJson,  new JsonLdOptions());
							response.outputStream << JsonUtils.toPrettyString(compact)
						}  else if(outCmd==OUTCMD_FRAME) {
							Object framed =  JsonLdProcessor.frame(JsonUtils.fromString(baos.toString().replace('"@id" : "urn:x-arq:DefaultGraphNode",','')), contextJson, new JsonLdOptions());
							response.outputStream << JsonUtils.toPrettyString(framed)
						}
					}
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
			
			response.contentType = RESPONSE_CONTENT_TYPE
			
			Object contextJson = null;
			if(graphs!=null && graphs.listNames().hasNext()) {
						
				if(outCmd==OUTCMD_NONE) { 
					if(incGph==INCGPH_NO) {
						Model m = graphs.getNamedModel(graphs.listNames().next());
						RDFDataMgr.write(response.outputStream, m, RDFLanguages.JSONLD);		
					} else {
						RDFDataMgr.write(response.outputStream, graphs, RDFLanguages.JSONLD);
					}
				} else {
					if(contextJson==null) {
						if(outCmd==OUTCMD_CONTEXT) {
							contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey, grailsApplication.config.annotopia.jsonld.openannotation.context));
						} else if(outCmd==OUTCMD_FRAME) {
							contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey,grailsApplication.config.annotopia.jsonld.openannotation.framing));						
						}
					}
				
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					if(incGph==INCGPH_NO) {			
						Model m = graphs.getNamedModel(graphs.listNames().next());
						RDFDataMgr.write(baos, m.getGraph(), RDFLanguages.JSONLD);			
					} else {
						RDFDataMgr.write(baos, graphs, RDFLanguages.JSONLD);
					}
					
					if(outCmd==OUTCMD_CONTEXT) {
						Object compact = JsonLdProcessor.compact(JsonUtils.fromString(baos.toString()), contextJson, new JsonLdOptions());
						response.outputStream << JsonUtils.toPrettyString(compact)
					}  else if(outCmd==OUTCMD_FRAME) {
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
		
		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}
		
		// Parsing the incoming parameters
		def outCmd = (request.JSON.outCmd!=null)?request.JSON.outCmd:OUTCMD_NONE;
		if(params.outCmd!=null) outCmd = params.outCmd;		
		def incGph = (request.JSON.incGph!=null)?request.JSON.incGph:INCGPH_NO;
		if(params.incGph!=null) incGph = params.incGph;
		if(outCmd==OUTCMD_FRAME && incGph==INCGPH_YES) {
			def message = "Invalid options outCmd=='frame' && incGph=='true', framing does not currently support Named Graphs";
			log.warn("[" + apiKey + "] " + message);
			render(status: 401, text: returnMessage(apiKey, "rejected", message, startTime),
				contentType: "text/json", encoding: "UTF-8");
			return;
		}

		// Unused but planned
		def flavor = (request.JSON.flavor!=null)?request.JSON.flavor:"OA";
		def validate = (request.JSON.validate!=null &&  request.JSON.validate in ['ON'])?request.JSON.validate:"OFF";
				
		// Parsing the payload
		def item = request.JSON.item
		if(item!=null) {
			if(request.JSON.validate=='ON')  log.warn("[" + apiKey + "] TODO: Validation of the Annotation content requested but not implemented yest!");
						
			// Reads the inputs in a dataset
			Dataset inMemoryDataset = DatasetFactory.createMem();
			try {
				RDFDataMgr.read(inMemoryDataset, new ByteArrayInputStream(item.toString().getBytes("UTF-8")), RDFLanguages.JSONLD);
			} catch (Exception ex) {
				log.error("[" + apiKey + "] " + ex.getMessage());
				def message = "Invalid content, annotation cannot be read";
				log.error("[" + apiKey + "] " + message + ": " + item.toString());
				render(status: 500, text: returnMessage(apiKey, "invalidcontent", message, startTime), contentType: "text/json", encoding: "UTF-8");
				return;
			}
			
			// Saves the annotation through services
			Dataset savedAnnotation;
			try {
				savedAnnotation = openAnnotationStorageService.saveAnnotationDataset(apiKey, startTime, Boolean.parseBoolean(incGph), inMemoryDataset);
				//println savedAnnotation.getDefaultGraph();
			} catch(StoreServiceException exception) {
				log.error("[" + apiKey + "] " + exception.getMessage());
				render(status: exception.status, text: exception.text, contentType: exception.contentType, encoding: exception.encoding);
				return;
			}
			
			try {
				if(savedAnnotation!=null) { 					
					renderSavedNamedGraphsDataset(apiKey, startTime, outCmd, 'saved', response, savedAnnotation);
				} else {
					// Annotation Set not found
					def message = "Null Dataset returned from annotation saving. Something went terribly wrong";
					render(status: 500, text: returnMessage(apiKey, "notfound", message, startTime), contentType: "text/json", encoding: "UTF-8");
					return;
				}
			} catch(Exception exception) {
				log.error("[" + apiKey + "] " + exception.getMessage());	
				def message = "Invalid content, saved annotation does not seem well formatted";
				log.error("[" + apiKey + "] " + message + ": " + getDatasetAsString(savedAnnotation));
				render(status: 500, text: returnMessage(apiKey, "invalidcontent", message, startTime), contentType: "text/json", encoding: "UTF-8");
				return;
			}
		} else {
			// Annotation not found
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
		
		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}
		
		def outCmd = (request.JSON.outCmd!=null)?request.JSON.outCmd:OUTCMD_NONE;
		if(params.outCmd!=null) outCmd = params.outCmd;
		def incGph = (request.JSON.incGph!=null)?request.JSON.incGph:INCGPH_NO;
		if(params.incGph!=null) incGph = params.incGph;
		if(outCmd==OUTCMD_FRAME && incGph==INCGPH_YES) {
			log.warn("[" + apiKey + "] Invalid options, framing does not currently support Named Graphs");
			def message = 'Invalid options, framing does not currently support Named Graphs';
			render(status: 401, text: returnMessage(apiKey, "rejected", message, startTime),
				contentType: "text/json", encoding: "UTF-8");
			return;
		}
		
		// Unused but planned
		def flavor = (request.JSON.flavor!=null)?request.JSON.flavor:"OA";
		def validate = (request.JSON.validate!=null &&  request.JSON.validate in ['ON'])?request.JSON.validate:"OFF";
		
		def item = request.JSON.item
				
		if(item!=null) {
			if(request.JSON.validate=='ON') log.warn("[" + apiKey + "] TODO: Validation of the Annotation content requested but not implemented yest!");
			
			Dataset inMemoryDataset = DatasetFactory.createMem();
			try {
				RDFDataMgr.read(inMemoryDataset, new ByteArrayInputStream(item.toString().getBytes("UTF-8")), RDFLanguages.JSONLD);
			} catch (Exception ex) {
				log.error("[" + apiKey + "] " + ex.getMessage());
				log.error("[" + apiKey + "] Invalid content: " + item.toString());
				def message = "Annotation cannot be read";
				render(status: 500, text: returnMessage(apiKey, "invalidcontent", message, startTime), contentType: "text/json", encoding: "UTF-8");
				return;
			}
			
			Dataset updatedAnnotation;
			try {
				updatedAnnotation = openAnnotationStorageService.updateAnnotationDataset(apiKey, startTime, Boolean.parseBoolean(incGph), inMemoryDataset);
			} catch(StoreServiceException exception) {
				render(status: exception.status, text: exception.text, contentType: exception.contentType, encoding: exception.encoding);
				return;
			}
			
			try {
				if(updatedAnnotation!=null) {
					renderSavedNamedGraphsDataset(apiKey, startTime, outCmd, 'updated', response, updatedAnnotation);
				} else {
					// Dataset returned null
					def message = "Null Dataset returned from annotation updating. Something went terribly wrong";
					render(status: 500, text: returnMessage(apiKey, "exception", message, startTime), contentType: "text/json", encoding: "UTF-8");
				}
			} catch(Exception ex) {
				log.error("[" + apiKey + "] " + ex.getMessage());
				ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
				RDFDataMgr.write(outputStream, updatedAnnotation, RDFLanguages.JSONLD);
				log.error("[" + apiKey + "] Invalid content: " + outputStream.toString());
				def message = "Updated annotation does not seem well formatted";
				render(status: 500, text: returnMessage(apiKey, "invalidcontent", message, startTime), contentType: "text/json", encoding: "UTF-8");
				return;
			}
		} else {
			// Annotation Set not found
			def message = "No annotation found in the request";
			render(status: 200, text: returnMessage(apiKey, "nocontent", message, startTime), contentType: "text/json", encoding: "UTF-8");
		}
	}
	
	def validate = {
		long startTime = System.currentTimeMillis();

		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}
		
		def item = request.JSON.item
		
		// Unused but planned
		def flavor = (request.JSON.flavor!=null)?request.JSON.flavor:"OA";

		if(item!=null) {
			List<HashMap<String,Object>> results = openAnnotationValidationService.validate(new ByteArrayInputStream(item.toString().getBytes("UTF-8")), "application/json");
			
			JSONObject jsonFinalSummary = new JSONObject();
			JSONObject annotationSummary = new JSONObject();
			
			JSONArray finalResult = new JSONArray();
			for(resultExternal in results) {
				//HashMap<String,Object> resultExternal = results.get(0);
				if(resultExternal.containsKey("result") && resultExternal.get("result")!=null) {
					
					JSONObject jsonSummary = new JSONObject();
					//jsonSummary.put("content", item);
					jsonSummary.put("total", resultExternal.get("total"));
					jsonSummary.put("model", resultExternal.get("model"));
					jsonSummary.put("warn", resultExternal.get("warn"));
					jsonSummary.put("error", resultExternal.get("error"));
					jsonSummary.put("skip", resultExternal.get("skip"));
					jsonSummary.put("pass", resultExternal.get("pass"));
					
					JSONObject jsonGraph = new JSONObject();
					jsonGraph.put("summary", jsonSummary);
					
					JSONArray jsonResults = new JSONArray();
					Object resultInternal = resultExternal.get("result");
					for(Object result: resultInternal) {
						JSONObject jsonResult = new JSONObject();
		
						jsonResult.put("section", result.getAt("section"));
						jsonResult.put("warn", result.getAt("warn"));
						jsonResult.put("error", result.getAt("error"));
						jsonResult.put("skip", result.getAt("skip"));
						jsonResult.put("pass", result.getAt("pass"));
						jsonResult.put("total", result.getAt("total"));
						
						JSONArray jsonConstraints = new JSONArray();
						ArrayList constraints = result.getAt("constraints");
						for(Object constraint: constraints) {
							JSONObject jsonConstraint = new JSONObject();
							jsonConstraint.put("ref", constraint.getAt("ref"));
							jsonConstraint.put("url", constraint.getAt("url"));
							jsonConstraint.put("severity", constraint.getAt("severity"));
							jsonConstraint.put("status", constraint.getAt("status"));
							jsonConstraint.put("result", constraint.getAt("result"));
							jsonConstraint.put("description", constraint.get("description"));
							jsonConstraints.add(jsonConstraint);
							jsonResult.put("constraints", jsonConstraints);
						}
						jsonResults.add(jsonResult);
					}
					jsonGraph.put("details", jsonResults);
					finalResult.add(jsonGraph);
				} else if(resultExternal.containsKey("totalGraphs") && resultExternal.get("totalGraphs")!=null) {
					jsonFinalSummary.put("graphs", resultExternal.get("totalGraphs"));
				} else if(resultExternal.containsKey("annotationGraphs") && resultExternal.get("annotationGraphs")!=null) {
					annotationSummary.put("graphs", resultExternal.get("annotationGraphs"));
				} else if(resultExternal.get("exception")!=null) {
					Object exc =  resultExternal.get("exception");
					
					JSONObject jsonException = new JSONObject();
					jsonException.put("step", "validation");
					jsonException.put("label", exc.get("label"));
					jsonException.put("description", exc.get("message"));
					
					finalResult.put("exception", jsonException);
				}
			}
			
			//jsonFinalSummary.put("totalGraphs", results.size());
			annotationSummary.put("validation", finalResult)
			jsonFinalSummary.put("annotation", annotationSummary);
			render jsonFinalSummary as JSON;
		}  else {
			// Annotation Set not found
			def message = "No annotation found in the request";
			render(status: 200, text: returnMessage(apiKey, "nocontent", message, startTime), contentType: "text/json", encoding: "UTF-8");
		}
	}
	
	def delete = {
		long startTime = System.currentTimeMillis();

		// Verifying the API key
		def apiKey = request.JSON.apiKey;
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr()); return;
		}

		def uri = (request.JSON.uri!=null)?request.JSON.uri:getCurrentUrl(request);
		log.info("[" + apiKey + "] Deleting annotation " + uri);
		Dataset graphs =  openAnnotationVirtuosoService.retrieveAnnotation(apiKey, uri);
		if(graphs!=null) {
			graphs.listNames().each {
				log.trace("[" + apiKey + "] Deleting graph " + it);
				jenaVirtuosoStoreService.dropGraph(apiKey, it);
				jenaVirtuosoStoreService.removeAllTriples(apiKey, grailsApplication.config.annotopia.storage.uri.graph.provenance, it);
			}
			def message = "Annotation deleted";
			render(status: 200, text: returnMessage(apiKey, "deleted", message, startTime), contentType: "text/json", encoding: "UTF-8");
		}  else {
			// Annotation Set not found
			def message = "Annotation not found";
			render(status: 200, text: returnMessage(apiKey, "notfound", message, startTime), contentType: "text/json", encoding: "UTF-8");
		}
	}
	
	private void renderSavedNamedGraphsDataset(def apiKey, long startTime, String outCmd, String status, HttpServletResponse response, Dataset dataset) {
		response.contentType = RESPONSE_CONTENT_TYPE
		
		// Count graphs
		int sizeDataset = 0;
		Iterator iterator = dataset.listNames();
		while(iterator.hasNext()) {
			sizeDataset++;
			iterator.next();
		}
		
		if(sizeDataset>1 && outCmd==OUTCMD_FRAME) {
			log.warn("[" + apiKey + "] Invalid options, framing does not currently support Named Graphs");
			def message = 'Invalid options, framing does not currently support Named Graphs';
			render(status: 401, text: returnMessage(apiKey, "rejected", message, startTime),
				contentType: "text/json", encoding: "UTF-8");
			return;
		}
		
		Dataset datasetToRender = DatasetFactory.createMem();
		if(sizeDataset==1) datasetToRender.setDefaultModel(dataset.getNamedModel(dataset.listNames().next()));
		else datasetToRender = dataset;
		
		def summaryPrefix = '"duration": "' + (System.currentTimeMillis()-startTime) + 'ms","graphs":"' + sizeDataset +  '",' + '"item":[';
		response.outputStream << '{"status":"' + status + '", "result": {' + summaryPrefix
		
		Object contextJson = null;
		if(outCmd==OUTCMD_NONE) {
			RDFDataMgr.write(response.outputStream, datasetToRender, RDFLanguages.JSONLD);
		} else {
			if(contextJson==null) {
				if(outCmd==OUTCMD_CONTEXT) {
					contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey, grailsApplication.config.annotopia.jsonld.openannotation.context));
				} else if(sizeDataset==1 && outCmd==OUTCMD_FRAME) {
					contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey, grailsApplication.config.annotopia.jsonld.openannotation.framing));
				}
			}
		
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			RDFDataMgr.write(baos, datasetToRender, RDFLanguages.JSONLD);
			
			if(outCmd==OUTCMD_CONTEXT) {
				Object compact = JsonLdProcessor.compact(JsonUtils.fromString(baos.toString()), contextJson, new JsonLdOptions());
				response.outputStream << JsonUtils.toPrettyString(compact)
			} else if(sizeDataset==1 && outCmd==OUTCMD_FRAME) {
				Object framed =  JsonLdProcessor.frame(JsonUtils.fromString(baos.toString().replace('"@id" : "urn:x-arq:DefaultGraphNode",','')), contextJson, new JsonLdOptions());
				response.outputStream << JsonUtils.toPrettyString(framed)
			} 
		}
		response.outputStream << ']}}'
		response.outputStream.flush()
	}
}
