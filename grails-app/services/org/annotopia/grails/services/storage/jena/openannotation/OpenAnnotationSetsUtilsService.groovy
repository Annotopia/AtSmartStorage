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
package org.annotopia.grails.services.storage.jena.openannotation

import javax.servlet.http.HttpServletResponse

import org.annotopia.grails.vocabularies.AnnotopiaVocabulary
import org.annotopia.grails.vocabularies.OA
import org.annotopia.grails.vocabularies.RDF
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFLanguages
import org.codehaus.groovy.grails.web.json.JSONObject

import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import com.github.jsonldjava.utils.JsonUtils
import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.DatasetFactory
import com.hp.hpl.jena.query.QueryExecution
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.query.ResultSet
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.Property
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.rdf.model.ResourceFactory

/**
 * This is providing utilities for querying in memory data according to the Annotopia
 * Annotation Sets extensions over Open Annotation.
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class OpenAnnotationSetsUtilsService {

	def grailsApplication
	def configAccessService
	def jenaUtilsService
	def jenaVirtuosoStoreService

	//	public boolean isAnnotationInDefaultGraphAlreadyStored(apiKey, Dataset dataset, String annotationUri) {
	//		log.info("[" + apiKey + "] Detecting if the Annotation has never been saved before " + annotationUri);
	//
	//		String QUERY = "PREFIX oa: <http://www.w3.org/ns/oa#> PREFIX pav: <http://purl.org/pav/> ASK { <" + annotationUri + "> a oa:Annotation . <" + annotationUri + "> pav:lastUpdateOn ?otherValue }"
	//		log.trace("[" + apiKey + "] Query: " + QUERY);
	//
	//		QueryExecution vqe = QueryExecutionFactory.create (QUERY, dataset);
	//		boolean result =  vqe.execAsk();
	//		log.info 'Result: ' + result;
	//	}

	public boolean isAnnotationChanged(apiKey, Dataset dataset, String annotationUri) {
		log.info("[" + apiKey + "] Detecting if the Annotation has never been saved before " + annotationUri);

		String QUERY = "PREFIX oa: <http://www.w3.org/ns/oa#> PREFIX at: <http://purl.org/annotopia#> ASK { <" + annotationUri + "> a oa:Annotation . <" + annotationUri + "> at:hasChanged ?flag ." +
				" FILTER (?flag = 'true') }"
		log.trace("[" + apiKey + "] Query: " + QUERY);

		QueryExecution vqe = QueryExecutionFactory.create (QUERY, dataset);
		boolean result =  vqe.execAsk();
		log.info 'Result: ' + result;
		result
	}

	/**
	 * Detects the uri(s) of at:AnnotationSet within a default graph.
	 * @param apiKey			The API key of the client that issued the request
	 * @param dataset			The dataset containing the annotations
	 * @param annotationSetUris The URIs for the annotation sets in the default graph
	 * @param closure			This closure is executed after the query.
	 * @return Number of at:AnnotationSet URIs detected within the default graph.
	 */
	public int detectAnnotationSetUriInDefaultGraph(apiKey, Dataset dataset, Set<Resource> annotationSetUris, Closure closure) {
		log.info("[" + apiKey + "] Detection of Annotation Set URI in Default Graph...");

		String QUERY = "PREFIX at: <http://purl.org/annotopia#> SELECT DISTINCT ?s WHERE { ?s a at:AnnotationSet . }"
		log.trace("[" + apiKey + "] Query: " + QUERY);

		int annotationsSetsUrisInDefaultGraphsCounter = 0;
		QueryExecution queryExecution  = QueryExecutionFactory.create (QueryFactory.create(QUERY), dataset);
		ResultSet rAnnotationsInDefaultGraph = queryExecution.execSelect();
		while (rAnnotationsInDefaultGraph.hasNext()) {
			Resource annUri = rAnnotationsInDefaultGraph.nextSolution().getResource("s");
			annotationsSetsUrisInDefaultGraphsCounter++;
			annotationSetUris.add(annUri);

			// Alters the triples with the Annotation as subject
			if(closure!=null)  closure(dataset.getDefaultModel(), annUri);
		}

		if(annotationsSetsUrisInDefaultGraphsCounter>0) log.info("[" + apiKey + "] Annotation Sets in Default Graph detected: " + annotationsSetsUrisInDefaultGraphsCounter);
		else log.info("[" + apiKey + "] No Annotation Set in Default Graph detected");
		annotationsSetsUrisInDefaultGraphsCounter
	}

	private InputStream callExternalUrl(def apiKey, String URL) {
		Proxy httpProxy = null;
		if(grailsApplication.config.annotopia.server.proxy.host && grailsApplication.config.annotopia.server.proxy.port) {
			String proxyHost = configAccessService.getAsString("annotopia.server.proxy.host"); //replace with your proxy server name or IP
			int proxyPort = configAccessService.getAsString("annotopia.server.proxy.port").toInteger(); //your proxy server port
			SocketAddress addr = new InetSocketAddress(proxyHost, proxyPort);
			httpProxy = new Proxy(Proxy.Type.HTTP, addr);
		}

		if(httpProxy!=null) {
			long startTime = System.currentTimeMillis();
			log.info ("[" + apiKey + "] " + "Proxy request: " + URL);
			URL url = new URL(URL);
			//Pass the Proxy instance defined above, to the openConnection() method
			URLConnection urlConn = url.openConnection(httpProxy);
			urlConn.connect();
			log.info ("[" + apiKey + "] " + "Proxy resolved in (" + (System.currentTimeMillis()-startTime) + "ms)");
			return urlConn.getInputStream();
		} else {
			log.info ("[" + apiKey + "] " + "No proxy request: " + URL);
			return new URL(URL).openStream();
		}
	}

	/**
	 * Mints a URI that is shaped according to the passed type.
	 * @param uriType	The type of the URI (graph, annotation, ...)
	 * @return The minted URI
	 */
	public String mintUri(uriType) {
		return configAccessService.getAsString("grails.server.protocol") + '://' +
				configAccessService.getAsString("grails.server.host") + ':' +
				configAccessService.getAsString("grails.server.port") + '/s/' + uriType + '/' +
				org.annotopia.grails.services.storage.utils.UUID.uuid();
	}

	/**
	 * Mints a URI of type graph
	 * @return The newly minted graph URI
	 */
	public String mintGraphUri() {
		return mintUri("graph");
	}

	/**
	 * Mints a URI of type annotation
	 * @return The newly minted annotation URI
	 */
	public String mintAnnotationUri() {
		return mintUri("annotation");
	}

	public Dataset splitAnnotationGraphs(apiKey, Dataset annotationSet) {
		String AT_FRAME = configAccessService.getAsString("annotopia.jsonld.annotopia.framing");
		log.info("[" + apiKey + "] Splitting of Annotation Sets Annotations into Named Graph...");

		// Count graphs
		int sizeDataset = 0;
		Iterator iterator = annotationSet.listNames();
		while(iterator.hasNext()) {
			sizeDataset++;
			iterator.next();
		}

		Dataset datasetToRender = DatasetFactory.createMem();
		if(sizeDataset==1) datasetToRender.setDefaultModel(annotationSet.getNamedModel(annotationSet.listNames().next()));

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		RDFDataMgr.write(baos, datasetToRender, RDFLanguages.JSONLD);

		Object contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey, AT_FRAME));
		Map<String, Object> framed =  JsonLdProcessor.frame(JsonUtils.fromString(baos.toString().replace('"@id" : "urn:x-arq:DefaultGraphNode",','')), contextJson, new JsonLdOptions());

		def annGraphUris = [];
		Dataset annotationGraphs = DatasetFactory.createMem();
		if(framed.get("@graph").getAt(0).getAt("annotations") instanceof java.util.ArrayList) {
			for(int i=0; i<framed.get("@graph").getAt(0).getAt("annotations").size(); i++) {
				println 'annotation ' + i + " - "
				def annotation = framed.get("@graph").getAt(0).getAt("annotations").get(i);
				annotation.put("@context", configAccessService.getAsString("annotopia.jsonld.annotopia.context"));

				String graphUri = mintGraphUri();
				annGraphUris.add(graphUri);
				Model model = ModelFactory.createDefaultModel();

				RDFDataMgr.read(model, new ByteArrayInputStream(JsonUtils.toString(annotation).getBytes("UTF-8")), RDFLanguages.JSONLD);
				annotationGraphs.addNamedModel(graphUri, model);
				framed.get("@graph").getAt(0).getAt("annotations").get(i).clear();
			}
		} else {
			def annotation = framed.get("@graph").getAt(0).getAt("annotations");
			annotation.put("@context", configAccessService.getAsString("annotopia.jsonld.annotopia.context"));

			String graphUri = mintGraphUri();
			annGraphUris.add(graphUri);
			Model model = ModelFactory.createDefaultModel();

			RDFDataMgr.read(model, new ByteArrayInputStream(JsonUtils.toString(annotation).getBytes("UTF-8")), RDFLanguages.JSONLD);
			annotationGraphs.addNamedModel(graphUri, model);
			framed.get("@graph").getAt(0).remove("annotations");
			framed.get("@graph").getAt(0).put("annotations", new ArrayList())
		}

		annGraphUris.each { annGraphUri ->
			framed.get("@graph").getAt(0).getAt("annotations").add(annGraphUri)
		}

		def isolatedSet = framed.get("@graph").getAt(0);
		isolatedSet.put("@context", configAccessService.getAsString("annotopia.jsonld.annotopia.context"));
		String setGraphUri = mintGraphUri();

		println JsonUtils.toString(isolatedSet)

		Model model = ModelFactory.createDefaultModel();
		RDFDataMgr.read(model, new ByteArrayInputStream(JsonUtils.toString(isolatedSet).getBytes("UTF-8")), RDFLanguages.JSONLD);
		annotationGraphs.addNamedModel(setGraphUri, model);
		annotationGraphs
	}

	private void detectAllBodiesTriples(String apiKey, Resource annotationResource, Dataset annotationSet, Model model) {
		String QUERY = "PREFIX oa:   <http://www.w3.org/ns/oa#> SELECT DISTINCT ?s WHERE { <" + annotationResource.getURI() + "> oa:hasBody ?s .  ?s ?p ?o. }"
		log.trace("[" + apiKey + "] Query: " + QUERY);

		QueryExecution queryExecution  = QueryExecutionFactory.create (QueryFactory.create(QUERY), annotationSet);
		ResultSet rAnnotationsInDefaultGraph = queryExecution.execSelect();
		while (rAnnotationsInDefaultGraph.hasNext()) {
			QuerySolution annotationSolution = rAnnotationsInDefaultGraph.nextSolution();

			Resource annUri = annotationSolution.getResource("s");
			Property predicate = (Property) annotationSolution.getResource("p");
			Resource object = annotationSolution.getResource("o");

			model.add(annotationResource, predicate, object);
		}
	}


	public void renderSavedNamedGraphsDataset(def apiKey, long startTime, String outCmd, String status, HttpServletResponse response, Dataset dataset) {
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
					contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey,  configAccessService.getAsString("annotopia.jsonld.annotopia.context")));
				} else if(sizeDataset==1 && outCmd=='frame') {
					contextJson = JsonUtils.fromInputStream(callExternalUrl(apiKey, configAccessService.getAsString("annotopia.jsonld.annotopia.framing")));
				}
			}

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			RDFDataMgr.write(baos, datasetToRender, RDFLanguages.JSONLD);

			if(outCmd=='context') {
				Object compact = JsonLdProcessor.compact(JsonUtils.fromString(baos.toString()), contextJson, new JsonLdOptions());
				response.outputStream << JsonUtils.toPrettyString(compact)
			}  else if(sizeDataset==1 && outCmd=='frame') {
				Object framed =  JsonLdProcessor.frame(JsonUtils.fromString(baos.toString().replace('"@id" : "urn:x-arq:DefaultGraphNode",','')), contextJson, new JsonLdOptions());
				response.outputStream << JsonUtils.toPrettyString(framed)
			}
		}
		response.outputStream << ']}}'
		response.outputStream.flush()
	}

	/**
	 * Render a JSON-LD representation of the annotation set and its annotations
	 * @param apiKey			The API key of the client issuing the request
	 * @param annotationSet		A Dataset object containing the annotation set's graph
	 * @param annotationSetURI	The URI of the annotation set
	 * @param response			The Grails response object
	 * @param status			The HTTP status code
	 */
	def renderAnnotationSet(def apiKey, def annotationSet, def annotationSetURI, def response, def status) {
		def defaultModel = annotationSet.getDefaultModel()

		// Get annotation graphs and merge into set
		def annotationSetModel = annotationSet.getNamedModel(annotationSet.listNames().next())
		def annotationURIs = []

		annotationSetModel.listStatements(null, ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS), null).each { statement ->
			def annGraphURI = statement.getObject().asResource().getURI()
			Dataset ds = jenaVirtuosoStoreService.retrieveGraph(apiKey, annGraphURI)
			Model annotationModel = ds.getNamedModel(ds.listNames().next())
			annotationModel.listStatements(null, ResourceFactory.createProperty(RDF.RDF_TYPE), ResourceFactory.createResource(OA.ANNOTATION)).each { annStatement ->
				annotationURIs.push(annStatement.getSubject())
			}
			defaultModel.add(annotationModel)
		}

		// Need to replace annotation graph URI with annotation URI so framing works later on...
		annotationSetModel.removeAll(null, ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS), null)
		annotationURIs.each { uri ->
			annotationSetModel.add(ResourceFactory.createResource(annotationSetURI),
					ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS),
					uri)
		}
		defaultModel.add(annotationSetModel)

		// Serialize into JSON-LD
		ByteArrayOutputStream baos = new ByteArrayOutputStream()
		RDFDataMgr.write(baos, defaultModel, RDFLanguages.JSONLD)

		// Frame
		def json = new JSONObject(baos.toString())
		json.remove("@id") // We have to do this for the framing to work for some reason...
		def contextJson = JsonUtils.fromInputStream(
				callExternalUrl(apiKey, configAccessService.getAsString("annotopia.jsonld.annotopia.framing")))
		def framed = JsonLdProcessor.frame(
				json,
				contextJson,
				new JsonLdOptions())

		// Remove wrapping @graph
		def framedJson = new JSONObject(framed)
		def outputJson = framedJson.get("@graph").getAt(0)
		outputJson.put('@context', framedJson.get('@context'))

		// Render response
		response.contentType = "text/json;charset=UTF-8"
		response.setStatus(status)
		response.outputStream << JsonUtils.toPrettyString(outputJson)
		response.outputStream.flush()
	}
}
