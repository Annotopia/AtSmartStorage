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

import grails.converters.JSON

import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFLanguages

import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import com.github.jsonldjava.utils.JSONUtils
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

/**
 * This is providing utilities for querying in memory data according to the Annotopia
 * Annotation Sets extensions over Open Annotation.
 * 
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class OpenAnnotationSetsUtilsService {

	def grailsApplication
	
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
			String proxyHost = grailsApplication.config.annotopia.server.proxy.host; //replace with your proxy server name or IP
			int proxyPort = grailsApplication.config.annotopia.server.proxy.port.toInteger(); //your proxy server port
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
		return 'http://' +
			grailsApplication.config.grails.server.host + ':' +
			grailsApplication.config.grails.server.port.http + '/s/' + uriType + '/' +
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
		String AT_FRAME = "https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/AnnotopiaFrame.json";
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

		Object contextJson = JSONUtils.fromInputStream(callExternalUrl(apiKey, AT_FRAME));
		Object framed =  JsonLdProcessor.frame(JSONUtils.fromString(baos.toString()), contextJson, new JsonLdOptions());
		String set = JSONUtils.toString(framed)

		def jsonSet = JSON.parse(set);
				
		def annGraphUris = [];
		
		Dataset annotationGraphs = DatasetFactory.createMem();
		for(int i=0; i<jsonSet['@graph'][0].annotations.length(); i++) {
			def annotation = jsonSet['@graph'][0].annotations.get(i);
			annotation.put("@context", "https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/AnnotopiaContext.json");
			
			String graphUri = mintGraphUri();
			annGraphUris.add(graphUri);
			Model model = ModelFactory.createDefaultModel();
			RDFDataMgr.read(model, new ByteArrayInputStream(JSONUtils.toString(annotation).getBytes("UTF-8")), RDFLanguages.JSONLD);
			annotationGraphs.addNamedModel(graphUri, model);
			
			
			
			println 'inside: ' + JSONUtils.toString(jsonSet['@graph'][0]);
		}
		
		jsonSet['@graph'][0].annotations.clear();
		
		annGraphUris.each { annGraphUri ->
			jsonSet['@graph'][0].annotations.add(annGraphUri)
		}

		def isolatedSet = jsonSet['@graph'][0];
		isolatedSet.put("@context", "https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/AnnotopiaContext.json");
		String setGraphUri = mintGraphUri();
		
		println JSONUtils.toString(isolatedSet)
		
		Model model = ModelFactory.createDefaultModel();
		RDFDataMgr.read(model, new ByteArrayInputStream(JSONUtils.toString(isolatedSet).getBytes("UTF-8")), RDFLanguages.JSONLD);
		annotationGraphs.addNamedModel(setGraphUri, model);
		
//		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//		RDFDataMgr.write(outputStream, model, RDFLanguages.JSONLD);
//		println "Set: " + outputStream.toString();
		
		annotationGraphs
		
		/*
		annotationGraphs.listNames().each {
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			RDFDataMgr.write(outputStream, annotationGraphs.getNamedModel(it), RDFLanguages.JSONLD);
			println "Annotation: " + outputStream.toString();
		}
		*/
		
		
		
//		Map<Resource, Resource> annotationSetUris = new HashMap<Resource, Resource>();
//		
//		String QUERY = "PREFIX oa:   <http://www.w3.org/ns/oa#> SELECT ?s ?p ?o WHERE { GRAPH ?g { ?s a oa:Annotation . ?s ?p ?o. }}"
//		log.trace("[" + apiKey + "] Query: " + QUERY);
//		
//		int annotationsCounter = 0;
//		QueryExecution queryExecution  = QueryExecutionFactory.create (QueryFactory.create(QUERY), annotationSet);
//		ResultSet rAnnotationsInDefaultGraph = queryExecution.execSelect();
//		println 'yoloy'
//		while (rAnnotationsInDefaultGraph.hasNext()) {
//			annotationsCounter++;
//			Model annotationModel = ModelFactory.createDefaultModel();
//			QuerySolution annotationSolution = rAnnotationsInDefaultGraph.nextSolution();
//			Resource annUri = annotationSolution.getResource("s");
//			Property predicate = (Property) annotationSolution.get("p");
//			Resource object = annotationSolution.getResource("o");
//			
//			annotationModel.add(annUri, predicate, object);
//			
//			
//			detectAllBodiesTriples(apiKey, annUri, annotationSet, annotationModel);
//			// Create a Model with all the Annotation things
//			// Add annotatedBy to the model
//			// Add bodies
//			// Add targets
//			
//			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//			RDFDataMgr.write(outputStream, annotationModel, RDFLanguages.JSONLD);
//			println outputStream.toString();
//		}
		
		
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
}
