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
package org.annotopia.grails.controllers.annotation.integrated

import grails.converters.JSON

import javax.servlet.http.HttpServletResponse

import org.annotopia.grails.vocabularies.AnnotopiaVocabulary
import org.annotopia.grails.vocabularies.OA
import org.annotopia.grails.vocabularies.RDF
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
import com.hp.hpl.jena.rdf.model.ResourceFactory
import com.hp.hpl.jena.rdf.model.Statement
import com.hp.hpl.jena.rdf.model.StmtIterator

/**
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class AnnotationIntegratedController extends BaseController {

	//String AT_CONTEXT = "https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/AnnotopiaContext.json";
	//String AT_FRAME = "https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/AnnotopiaFrame.json";
	//String AT_FRAME_LIGHT = "https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/AnnotopiaFrameLight.json";

	// outCmd (output command) constants
	private final OUTCMD_NONE = "none";
	private final OUTCMD_FRAME = "frame";
	private final OUTCMD_CONTEXT = "context";

	// incGph (Include graph) constants
	private final INCGPH_YES = "true";
	private final INCGPH_NO = "false";

	def grailsApplication;
	def configAccessService;

	def jenaUtilsService;
	def jenaVirtuosoStoreService;
	def apiKeyAuthenticationService;
	def openAnnotationSetStorageService;
	def openAnnotationVirtuosoService;
	def openAnnotationSetVirtuosoService

	def annotationIntegratedStorageService;
	def openAnnotationSetsUtilsService

	// Shared variables/functionality
	def startTime
	def apiKey
	def outCmd
	def incGph
	def beforeInterceptor = {
		startTime = System.currentTimeMillis()

		// Authenticate
		apiKey = apiKeyAuthenticationService.getApiKey(request)
		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr())
			return false // Returning false stops the actual controller action from being called
		}
		log.info("API key [" + apiKey + "]")

		// Response format parametrization and constraints
		outCmd = (request.JSON.outCmd!=null)?request.JSON.outCmd:OUTCMD_NONE;
		if(params.outCmd!=null) outCmd = params.outCmd;

		incGph = (request.JSON.incGph!=null)?request.JSON.incGph:INCGPH_NO;
		if(params.incGph!=null) incGph = params.incGph;

		if(outCmd==OUTCMD_FRAME && incGph==INCGPH_YES) {
			log.warn("[" + apiKey + "] Invalid options, framing does not currently support Named Graphs");
			def message = 'Invalid options, framing does not currently support Named Graphs';
			render(status: 401, text: returnMessage(apiKey, "rejected", message, startTime),
				contentType: "text/json", encoding: "UTF-8");
			return false;
		}
	}

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
	def showAnnotationSet = {
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
				tgtUrls = jenaVirtuosoStoreService.retrieveAllManifestationsByIdentifiers(apiKey, identifiers, configAccessService.getAsString("annotopia.storage.uri.graph.identifiers"));
			}

			int annotationSetsTotal = annotationIntegratedStorageService.countAnnotationSetGraphs(apiKey, tgtUrls, tgtFgt);
			int annotationSetsPages = (annotationSetsTotal/Integer.parseInt(max));
			if(annotationSetsTotal>0 && Integer.parseInt(offset)>0 && Integer.parseInt(offset)>=annotationSetsPages) {
				def message = 'The requested page ' + offset +
					' does not exist, the page index limit is ' + (annotationSetsPages==0?"0":(annotationSetsPages-1));
				render(status: 401, text: returnMessage(apiKey, "rejected", message, startTime),
					contentType: "text/json", encoding: "UTF-8");
				return;
			}

			Set<Dataset> annotationSets = annotationIntegratedStorageService.listAnnotationSets(apiKey, max, offset, tgtUrls, tgtFgt, tgtExt, tgtIds, incGph);
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
								contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey, configAccessService.getAsString("annotopia.jsonld.annotopia.context")));
							} else if(outCmd=='frame') {
								contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey, configAccessService.getAsString(".annotopia.jsonld.annotopia.framinglight")));
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
							Object framed =  JsonLdProcessor.frame(JsonUtils.fromString(baos.toString().replace('"@id" : "urn:x-arq:DefaultGraphNode",','')), contextJson, new JsonLdOptions());
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
			String url = null;
			if(getCurrentUrl(request).indexOf("?")>0) url = getCurrentUrl(request).substring(0, getCurrentUrl(request).indexOf("?"))
			else url = getCurrentUrl(request);

			Dataset graphs =  annotationIntegratedStorageService.retrieveAnnotationSet(apiKey, url);
			if(graphs!=null && graphs.listNames().hasNext()) {
				Set<Model> toAdd = new HashSet<Model>();
				Set<Statement> statsToAdd = new HashSet<Statement>();
				Set<Statement> toRemove = new HashSet<Statement>();
				Model setModel = graphs.getNamedModel(graphs.listNames().next());
				StmtIterator  iter = setModel.listStatements(null, ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS), null);
				while(iter.hasNext()) {
					Statement s = iter.next();
					toRemove.add(s);
					Dataset ds = jenaVirtuosoStoreService.retrieveGraph(apiKey, s.getObject().asResource().getURI());
					if(ds!=null && ds.listNames().hasNext()) {
						Model annotationModel = ds.getNamedModel(ds.listNames().next());
						StmtIterator  iter2 = annotationModel.listStatements(null, ResourceFactory.createProperty(RDF.RDF_TYPE), ResourceFactory.createResource(OA.ANNOTATION));
						if(iter2.hasNext()) {
							toAdd.add(annotationModel);
							//setModel.add(annotationModel);
							statsToAdd.add(ResourceFactory.createStatement(s.getSubject(), ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS), iter2.next().getSubject()));
						}
					}
				}
				toRemove.each {
					setModel.remove(it);
				}
				statsToAdd.each {
					setModel.add(it);
				}
				toAdd.each {
					setModel.add(it);
				}
			}

			Object contextJson = null;
			if(graphs!=null && graphs.listNames().hasNext()) {

				Model setModel = graphs.getNamedModel(graphs.listNames().next());

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
							contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey, configAccessService.getAsString("annotopia.jsonld.annotopia.context")));
						} else if(outCmd=='frame') {
							contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey, configAccessService.getAsString("annotopia.jsonld.annotopia.framing")));
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
						Object framed =  JsonLdProcessor.frame(JsonUtils.fromString(baos.toString().replace('"@id" : "urn:x-arq:DefaultGraphNode",','')),contextJson, new JsonLdOptions());
						response.outputStream << JsonUtils.toPrettyString(framed)
					}
				}
				response.outputStream.flush()
			} else {
				// Annotation Set not found
				def message = 'Annotation set ' + getCurrentUrl(request) + ' has not been found';
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
	def saveAnnotationSet = {
		log.info("[" + apiKey + "] Saving Annotation Set");

		// Parsing the incoming parameters
		def set = request.JSON.set

		// Unused but planned
		def flavor = (request.JSON.flavor!=null)?request.JSON.flavor:"OA";
		def validate = (request.JSON.validate!=null)?request.JSON.validate:"OFF";

		if(set!=null) {
			Dataset savedAnnotationSet;
			try {
				savedAnnotationSet = annotationIntegratedStorageService.saveAnnotationSet(apiKey, startTime, Boolean.parseBoolean(incGph), set.toString());
			} catch(StoreServiceException exception) {
				render(status: exception.status, text: exception.text, contentType: exception.contentType, encoding: exception.encoding);
				return;
			}

			if(savedAnnotationSet!=null) {
				openAnnotationSetsUtilsService.renderSavedNamedGraphsDataset(apiKey, startTime, outCmd, 'saved', response, savedAnnotationSet);
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
	def updateAnnotationSet = {
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
				updatedAnnotationSet = annotationIntegratedStorageService.updateAnnotationSet(apiKey, startTime, Boolean.parseBoolean(incGph), set.toString());
			} catch(StoreServiceException exception) {
				render(status: exception.status, text: exception.text, contentType: exception.contentType, encoding: exception.encoding);
				return;
			}

			Object contextJson = null;
			if(updatedAnnotationSet!=null) {
				openAnnotationSetsUtilsService.renderSavedNamedGraphsDataset(apiKey, startTime, outCmd, 'saved', response, updatedAnnotationSet);
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
}
