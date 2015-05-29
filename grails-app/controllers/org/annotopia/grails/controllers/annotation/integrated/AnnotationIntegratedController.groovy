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

import java.util.regex.Matcher

import org.annotopia.grails.vocabularies.AnnotopiaVocabulary
import org.annotopia.grails.vocabularies.OA
import org.annotopia.grails.vocabularies.PAV
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
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.ResultSet
import virtuoso.jena.driver.VirtGraph
import virtuoso.jena.driver.VirtuosoQueryExecution
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.Resource
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
	def openAnnotationUtilsService
	def openAnnotationStorageService

	// Shared variables/functionality
	def startTime
	def apiKey
	def outCmd
	def incGph
	def annotationSet
	def annotationSetURI

	def beforeInterceptor = {
		startTime = System.currentTimeMillis()

		// Authenticate with OAuth or Annotopia API Key
		def oauthToken = apiKeyAuthenticationService.getOauthToken(request)
		if(oauthToken != null) {
			apiKey = oauthToken.system.apikey
		} else {
			apiKey = apiKeyAuthenticationService.getApiKey(request)

			if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
				invalidApiKey(request.getRemoteAddr())
				return false // Returning false stops the actual controller action from being called
			}
		}
		log.info("API key [" + apiKey + "]")

		// Response format parametrization and constraints
		outCmd = (request.JSON.outCmd!=null)?request.JSON.outCmd:OUTCMD_FRAME;
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

		// Get annotation set if necessary
		if(actionName == 'update' || actionName == 'show') {
			// Figure out the URL of the annotation set
			annotationSetURI = null
			// Strip off the params to get the URL as it would appear as a named graph in the triplestore
			Matcher matcher = getCurrentUrl(request) =~ /(.*\/s\/annotationset\/[-a-zA-Z0-9]*).*/
			if(matcher.matches())
				annotationSetURI = matcher.group(1)

			// Fetch the annotation set
			annotationSet = null
			if(annotationSetURI != null) {
				log.info("Annotation set URI: " + annotationSetURI)
				annotationSet = annotationIntegratedStorageService.retrieveAnnotationSet(apiKey, annotationSetURI)
			}
			if(annotationSet == null || !annotationSet.listNames().hasNext()) {
				// Annotation Set not found
				def message = 'Annotation set ' + getCurrentUrl(request) + ' has not been found'

				render(status: 404, text: returnMessage(apiKey, "notfound", message, startTime), contentType: "text/json", encoding: "UTF-8")
				return false
			}
		}
	}

	def index = {
		// GET of a list of annotations
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

	/*
	 * GET
	 *
	 * GET of the annotation set identified by the current url
	 * Note that the content that is returned might not be the bare Annotation
	 * but more likely will be a Named Graph that wraps the annotation.
	 * If the annotation is related to multiple graphs, all of the graphs will
	 * be returned.
	 *
	 * The returned format is compliant with the Open Annotation specification
	 * http://www.openannotation.org/spec/core/
	 */
	def show = {
		// Filtering within sets
		if(params.tgtUrl!=null || params.tgtDoi!=null || params.tgtPmid!=null || params.tgtPmcid!=null) {
			// Get the names of the annotation graphs that contain
			//  annotations with targets that match the filter!
			StringBuffer queryBuffer = new StringBuffer()
			queryBuffer << "PREFIX oa:<http://www.w3.org/ns/oa#>\n" +
					"SELECT DISTINCT ?annotation_graph WHERE { graph ?g {" +
					"<"+annotationSetURI+"> <http://purl.org/annotopia#annotations> ?annotation_graph ." +
					"graph ?annotation_graph {" +
					"?s a <http://www.w3.org/ns/oa#Annotation> ."
			if(params.tgtUrl!=null)
				openAnnotationVirtuosoService.getTargetFilter(queryBuffer, [params.tgtUrl], "true")

			if(params.tgtDoi!=null)
				openAnnotationVirtuosoService.getTargetDoiFilter(queryBuffer, params.tgtDoi)

			if(params.tgtPmid!=null)
				openAnnotationVirtuosoService.getTargetPubMedFilter(queryBuffer, params.tgtPmid)

			if(params.tgtPmcid!=null)
				openAnnotationVirtuosoService.getTargetPubMedCentralFilter(queryBuffer, params.tgtPmcid)

			queryBuffer << "}}}"

			println queryBuffer

			// Remove all annotations references for now
			def model = annotationSet.getNamedModel(annotationSet.listNames().next())
			model.removeAll(ResourceFactory.createResource(annotationSetURI),
					ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS),
					null)
			// Do the filter query
			def graph = new VirtGraph (
					configAccessService.getAsString("annotopia.storage.triplestore.host"),
					configAccessService.getAsString("annotopia.storage.triplestore.user"),
					configAccessService.getAsString("annotopia.storage.triplestore.pass"))

			VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(QueryFactory.create(queryBuffer.toString()), graph);
			ResultSet filterMatches = vqe.execSelect();
			while (filterMatches.hasNext()) {
				// Re-add the annotations that matched the query
				Resource annGraphUri = filterMatches.nextSolution().getResource("annotation_graph")
				model.add(ResourceFactory.createResource(annotationSetURI),
						ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS),
						annGraphUri)
			}
		}

		openAnnotationSetsUtilsService.renderAnnotationSet(apiKey, annotationSet, response, 200)
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
	def create = {
		log.info("[" + apiKey + "] Saving Annotation Set");

		// Parsing the incoming parameters
		def set = request.JSON

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
				// Refetch the annotation set so it's in a form that is compatible with the renderer
				def savedAnnotationSetModel = savedAnnotationSet.getNamedModel(savedAnnotationSet.listNames().next())
				def setURI = savedAnnotationSetModel.listStatements(null,
						ResourceFactory.createProperty(RDF.RDF_TYPE),
						ResourceFactory.createResource(AnnotopiaVocabulary.ANNOTATION_SET)).next().getSubject().toString()
				savedAnnotationSet = annotationIntegratedStorageService.retrieveAnnotationSet(apiKey, setURI)

				openAnnotationSetsUtilsService.renderAnnotationSet(apiKey, savedAnnotationSet, response, 201)
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
		// TODO: Extract all this logic out into a service
		log.info("[" + apiKey + "] Updating Annotation Set");

		def annotationSetJson = request.JSON

		def jsonURI = annotationSetJson.get("@id")
		if(jsonURI != annotationSetURI) {
			def message = "The ID of the annotation set in the request URL ("+annotationSetURI+") does not match the one in the request body ("+jsonURI+")"
			log.error("[" + apiKey + "] " + message + ": " + annotationSetJson.toString())
			render(status: 500, text: returnMessage(apiKey, "invalidcontent", message, startTime), contentType: "text/json", encoding: "UTF-8")
			return
		}

		// Get the [graph URIs : URIs] of the annotations already in the set
		def existingAnnotationURIMap = annotationIntegratedStorageService.retrieveAnnotationUrisInSet(apiKey, annotationSetURI)

		// Snip out the "annotations" property
		List annotations = annotationSetJson.getAt("annotations")
		def storedAnnotationGraphURIs = []
		// Iterate over all the annotations given in the PUT and either update or create them, recording
		//  their graph URIs.
		annotations.each() {
			// Copy the @context node so the "snipped out" JSON-LD makes sense... must be a better way of doing this!
			def annotationJson = it.put("@context", annotationSetJson.get("@context"))
			def annotationDataset = DatasetFactory.createMem()
			try {
				RDFDataMgr.read(annotationDataset, new ByteArrayInputStream(annotationJson.toString().getBytes("UTF-8")), RDFLanguages.JSONLD)
			} catch (Exception ex) {
				log.error("[" + apiKey + "] " + ex.getMessage())
				def message = "Invalid content, the following annotation could not be read\n"
				log.error("[" + apiKey + "] " + message + ": " + annotationJson.toString())
				render(status: 500, text: returnMessage(apiKey, "invalidcontent", message, startTime), contentType: "text/json", encoding: "UTF-8")
				return
			}

			// Get the annotation's ID
			Set<Resource> annotationURIs = new HashSet<Resource>()
			def numAnnotations = openAnnotationUtilsService.detectAnnotationsInDefaultGraph(apiKey, annotationDataset, annotationURIs, null)

			// Hopefully there was only one annotation in the JSON-LD, but if not lets just pick the first one
			def annotationURI = null
			if(!annotationURIs.isEmpty())
				annotationURI = annotationURIs.first().toString()

			// Was the annotation already in the set?
			// TODO: check not part of another annotation set!
			def annotation
			if(annotationURI != null && existingAnnotationURIMap.remove(annotationURI) != null) {
				log.info("[" + apiKey + "] Updating annotation: " + annotationURI)
				annotation = openAnnotationStorageService.updateAnnotationDataset(apiKey, startTime, false, annotationDataset);
			} else {
				log.info("[" + apiKey + "] Adding new annotation")
				annotation = openAnnotationStorageService.saveAnnotationDataset(apiKey, startTime, false, annotationDataset);
			}
			// Record graph URI for each annotation
			// Hopefully the only named graph in the dataset is that of the annotation!
			storedAnnotationGraphURIs.push(annotation.listNames().next())
		}

		annotationSetJson.getAt("annotations").clear() // Don't need this anymore, annotations have been stored in their own graphs

		def newAnnotationSet = DatasetFactory.createMem() // Dataset for the updated annotation set
		def newAnnotationSetModel = ModelFactory.createDefaultModel() // Model for the updated annotation set
		def annotationSetGraphURI = annotationSet.listNames().next() // URI of the original set's graph

		// Read the PUT body
		try {
			RDFDataMgr.read(newAnnotationSetModel, new ByteArrayInputStream(annotationSetJson.toString().getBytes("UTF-8")), RDFLanguages.JSONLD)
		} catch (Exception ex) {
			log.error("[" + apiKey + "] " + ex.getMessage())
			def message = "Invalid content, the annotation set could not be read\n"
			log.error("[" + apiKey + "] " + message + ": " + annotationSetJson.toString())
			render(status: 500, text: returnMessage(apiKey, "invalidcontent", message, startTime), contentType: "text/json", encoding: "UTF-8")
			return
		}

		// Add annotation graph URIs to annotation set's "annotations" list
		newAnnotationSetModel.removeAll(ResourceFactory.createResource(annotationSetURI),
				ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS),
				null)
		storedAnnotationGraphURIs.each() {
			newAnnotationSetModel.add(ResourceFactory.createResource(annotationSetURI),
					ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS),
					ResourceFactory.createResource(it))
		}

		// Set Last saved on
		newAnnotationSetModel.removeAll(ResourceFactory.createResource(annotationSetURI),
				ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON),
				null)
		newAnnotationSetModel.add(ResourceFactory.createResource(annotationSetURI),
				ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON),
				ResourceFactory.createPlainLiteral(dateFormat.format(new Date())))

		// Add the updated annotation set as a named graph to the dataset
		newAnnotationSet.addNamedModel(annotationSetGraphURI, newAnnotationSetModel)

		// Store the updated annotation set dataset
		jenaVirtuosoStoreService.updateDataset(apiKey, newAnnotationSet)

		// Remove now-orphaned annotations
		existingAnnotationURIMap.values().each() {
			log.info("[" + apiKey + "] Deleting orphaned annotation graph: " + it);
			jenaVirtuosoStoreService.dropGraph(apiKey, it);
			jenaVirtuosoStoreService.removeAllTriples(apiKey, configAccessService.getAsString("annotopia.storage.uri.graph.provenance"), it);
			jenaVirtuosoStoreService.removeAllTriplesWithObject(apiKey, it);
		}

		// Render the set
		openAnnotationSetsUtilsService.renderAnnotationSet(apiKey, newAnnotationSet, response, 200)
	}
}
