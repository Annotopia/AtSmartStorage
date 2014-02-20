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
package org.annotopia.grails.services.storage.jena

import grails.converters.JSON

import org.annotopia.groovy.service.store.StoreServiceException
import org.apache.jena.riot.RDFDataMgr

import virtuoso.jena.driver.VirtGraph
import virtuoso.jena.driver.VirtuosoQueryExecution
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory

import com.github.jsonldjava.jena.JenaJSONLD
import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.DatasetFactory
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QueryExecution
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.query.ResultSet
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.Property
import com.hp.hpl.jena.rdf.model.RDFNode
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.rdf.model.ResourceFactory
import com.hp.hpl.jena.rdf.model.Statement
import com.hp.hpl.jena.rdf.model.StmtIterator

/**
 * This is the service that allows to manage annotation via Jena APIs.
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class AnnotationJenaStorageService {

	def grailsApplication
	def virtuosoJenaStoreService
	def openAnnotationUtilsService
	
	/**
	 * Lists the ann...
	 * @param apiKey
	 * @param max
	 * @param offset
	 * @param tgtUrl
	 * @param tgtFgt
	 * @param tgtExt
	 * @param tgtIds
	 * @return
	 */
	public listAnnotation(apiKey, max, offset, tgtUrl, tgtFgt, tgtExt, tgtIds) {
		log.info '[' + apiKey + '] List annotations' +
			' max:' + max +
			' offset:' + offset +
			' tgtUrl:' + tgtUrl +
			' tgtFgt:' + tgtFgt;
		retrieveAnnotationGraphs(apiKey, max, offset, tgtUrl, tgtFgt);
	}
	
	public Set<Dataset> retrieveAnnotationGraphs(apiKey, max, offset, tgtUrl, tgtFgt) {
		log.info '[' + apiKey + '] Retrieving annotation graphs';
	
//		Dataset graphs;
//		List<String> graphNames = retrieveAnnotationGraphsNames(apiKey, max, offset, tgtUrl, tgtFgt);
//		if(graphNames!=null) {
//			graphNames.each { graphName ->
//				if(graphs==null) graphs =  virtuosoJenaStoreService.retrieveGraph(apiKey, graphName);
//				else {
//					Dataset ds = virtuosoJenaStoreService.retrieveGraph(apiKey, graphName);
//					ds.listNames().each { name ->
//						graphs.addNamedModel(name, ds.getNamedModel(name));
//					}
//				}
//			}
//		}
		Set<Dataset> datasets = new HashSet<Dataset>();
		List<String> graphNames = retrieveAnnotationGraphsNames(apiKey, max, offset, tgtUrl, tgtFgt);
		if(graphNames!=null) {
			graphNames.each { graphName ->				
				Dataset ds = virtuosoJenaStoreService.retrieveGraph(apiKey, graphName);	
				if(ds!=null)			
				datasets.add(ds);
			}
		}
		return datasets;
	}
	
	public List<String> retrieveAnnotationGraphsNames(apiKey, max, offset, tgtUrl, tgtFgt) {
		log.info  '[' + apiKey + '] Retrieving annotation graphs names ' +
			' max:' + max +
			' offset:' + offset +
			' tgtUrl:' + tgtUrl +
			' tgtFgt:' + tgtFgt;
		
		VirtGraph set = new VirtGraph (
			grailsApplication.config.annotopia.storage.triplestore.host,
			grailsApplication.config.annotopia.storage.triplestore.user,
			grailsApplication.config.annotopia.storage.triplestore.pass);
		
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
			"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a oa:Annotation }} LIMIT " + max + " OFFSET " + offset;
		
		if(tgtUrl!=null && tgtFgt=="false") {
			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
				"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a oa:Annotation . ?s oa:hasTarget <" + tgtUrl + "> }} LIMIT " + max + " OFFSET " + offset;
		} else if(tgtUrl!=null && tgtFgt=="true") {
			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
				"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a oa:Annotation . {?s oa:hasTarget <" + tgtUrl +
				"> } UNION {?s oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource <" + tgtUrl + ">}}} LIMIT " + max + " OFFSET " + offset;
		}
			
		log.trace('[' + apiKey + '] ' + queryString);
		
		List<String> graphs = new ArrayList<String>();
		
		try {
			Query  sparql = QueryFactory.create(queryString);
			VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (sparql, set);
			
			ResultSet results = vqe.execSelect();
			while (results.hasNext()) {
				QuerySolution result = results.nextSolution();
				RDFNode graph_name = result.get("g");
				graphs.add(graph_name.toString());
			}
			return graphs;
		} catch (Exception e) {
			log.error(e.getMessage());
			return null;
		}
	}
	
	
	public listAnnotationSet(apiKey, max, offset, tgtUrl, tgtFgt, tgtExt, tgtIds) {
		log.info '[' + apiKey + '] List annotation' + 
			'max:' + max + 
			'offset:' + offset +
			'tgtUrl:' + tgtUrl +
			'tgtFgt:' + tgtFgt;
		retrieveAnnotationGraphs(apiKey, max, offset, tgtUrl, tgtFgt);
	}
	
	
	
	public int countAnnotationGraphs(apiKey, tgtUrl, tgtFgt) {
		VirtGraph set = new VirtGraph (
			grailsApplication.config.annotopia.storage.triplestore.host,
			grailsApplication.config.annotopia.storage.triplestore.user,
			grailsApplication.config.annotopia.storage.triplestore.pass);
		
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
			"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation }}";
			
		if(tgtUrl!=null && tgtFgt=="false") {
			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
				"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation . ?s oa:hasTarget <" + tgtUrl + "> }}";
		} else if(tgtUrl!=null && tgtFgt=="true") {
			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
				"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation .  {?s oa:hasTarget <" + tgtUrl +
				"> } UNION {?s oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource <" + tgtUrl + ">}}}";
		}
			
		log.trace('[' + apiKey + '] ' +  queryString);
			
		int totalCount = 0;
		Query  sparql = QueryFactory.create(queryString);
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (sparql, set);
		ResultSet results=vqe.execSelect();
		if(results.hasNext())
		{
			totalCount = Integer.parseInt(results.next().get("total").getString());
		}
		log.info('[' + apiKey + '] Total accessible Annotation Sets: ' + totalCount);
		totalCount;
	}
	
	
	
	public int countAnnotations(apiKey) {
		VirtGraph set = new VirtGraph (
			grailsApplication.config.annotopia.storage.triplestore.host,
			grailsApplication.config.annotopia.storage.triplestore.user,
			grailsApplication.config.annotopia.storage.triplestore.pass);
		
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
			"SELECT (COUNT(DISTINCT ?s) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation }}";
		log.trace('[' + apiKey + ']' +  queryString);
			
		int totalCount = 0;
		Query  sparql = QueryFactory.create(queryString);
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (sparql, set);
		ResultSet results=vqe.execSelect();
		if(results.hasNext())
		{
			totalCount = Integer.parseInt(results.next().get("total").getString());
		}
		log.info('[' + apiKey + '] Total accessible Annotations: ' + totalCount);
		totalCount;
	}
	
	
	
	
	
	public void storeAnnotationSet(apiKey, set, flavor, validate) {
		
		if(validate!='OFF') {
			log.warn("[" + apiKey + "] TODO: Validation of the annotation set content with flavor " + flavor + " requested but not implemented!");
		}
		
		virtuosoJenaStoreService.store(apiKey, set, "");
	}
	
	public void storeAnnotation(apiKey, annotation, flavor, validate) {
		
		if(validate!='OFF') {
			log.warn("[" + apiKey + "] TODO: Validation of the annotation content with flavor " + flavor + " requested but not implemented!");
		}
		
		virtuosoJenaStoreService.store(apiKey, annotation, "");
	}
	
	public void updateAnnotationSet(apiKey, graphUri, set, flavor, validate) {
		
		if(validate!='OFF') {
			log.warn("[" + apiKey + "] TODO: Validation of the annotation set content with flavor " + flavor + " requested but not implemented!");
		}
		
		virtuosoJenaStoreService.clearGraph(apiKey, graphUri);
		virtuosoJenaStoreService.store(apiKey, set, "");
	}
	
	public Set<String> retrieveAnnotationGraphNames(apiKey, uri) {
		
		log.info '[' + apiKey + '] Retrieving graph for annotation ' + uri;
		
			VirtGraph set = new VirtGraph (
				grailsApplication.config.annotopia.storage.triplestore.host,
				grailsApplication.config.annotopia.storage.triplestore.user,
				grailsApplication.config.annotopia.storage.triplestore.pass);
			
			String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
				"SELECT DISTINCT ?g WHERE { GRAPH ?g { <" + uri + "> a oa:Annotation }}";
				
			log.trace('[' + apiKey + '] ' + queryString);
			
			Set<String> graphs = new HashSet<String>();
			Query  sparql = QueryFactory.create(queryString);
			VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (sparql, set);
			
			try {
				ResultSet results = vqe.execSelect();
				while (results.hasNext()) {
					QuerySolution result = results.nextSolution();
					RDFNode graph_name = result.get("g");
					if(graph_name!=null) graphs.add(graph_name);
				}
				return graphs;
			} catch (Exception e) {
				log.error(e.getMessage());
				return null;
			}
	}
	
	public Dataset retrieveAnnotation(apiKey, uri) {

		log.info '[' + apiKey + '] Retrieving annotation ' + uri;
	
		VirtGraph set = new VirtGraph (
			grailsApplication.config.annotopia.storage.triplestore.host,
			grailsApplication.config.annotopia.storage.triplestore.user,
			grailsApplication.config.annotopia.storage.triplestore.pass);
		
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
			"SELECT DISTINCT ?g WHERE { GRAPH ?g { <" + uri + "> a oa:Annotation }}";
			
		log.trace('[' + apiKey + '] ' + queryString);
		
		Dataset graphs;
		Query  sparql = QueryFactory.create(queryString);
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (sparql, set);
		
		try {
			ResultSet results = vqe.execSelect();
			while (results.hasNext()) {
				QuerySolution result = results.nextSolution();
				RDFNode graph_name = result.get("g");
				if(graphs==null) graphs =  virtuosoJenaStoreService.retrieveGraph(apiKey, graph_name.toString()); 
				else {
					Dataset ds = virtuosoJenaStoreService.retrieveGraph(apiKey, graph_name.toString());
					ds.listNames().each { name ->
						graphs.addNamedModel(name, ds.getNamedModel(name));
					}
				}
			}
			return graphs;
		} catch (Exception e) {
			log.error(e.getMessage());
			return null;
		}
	}
	
	public Dataset retrieveAnnotationGraph(apiKey, uri) {
		log.info '[' + apiKey + '] Retrieving annotation graph';
	
		Dataset graphs;
		graphs =  virtuosoJenaStoreService.retrieveGraph(apiKey, uri);
		return graphs;
	}
	
	public Dataset saveAnnotationDataset(apiKey, startTime, Dataset dataset) {
		
		Set<Resource> annotationUris = new HashSet<Resource>();
		
		// Detection of default graph
		int annotationsInDefaultGraphsCounter = openAnnotationUtilsService.detectAnnotationsInDefaultGraph(apiKey, dataset, annotationUris)
		boolean defaultGraphDetected = (annotationsInDefaultGraphsCounter>0);
			
		
		// Detect all named graphs
		Set<Resource> graphsUris = openAnnotationUtilsService.detectNamedGraphs(apiKey, dataset);

		// Query for graphs containing annotation
		// See: https://www.mail-archive.com/wikidata-l@lists.wikimedia.org/msg00370.html
		Set<Resource> annotationsGraphsUris = new HashSet<Resource>();
		int detectedAnnotationGraphsCounter = openAnnotationUtilsService.detectAnnotationsInNamedGraph(apiKey, dataset, graphsUris, annotationsGraphsUris, annotationUris)
		
		if(defaultGraphDetected && detectedAnnotationGraphsCounter>0) {
			log.info("[" + apiKey + "] Mixed Annotation content detected... request rejected.");
			def json = JSON.parse('{"status":"nocontent" ,"message":"The request carries a mix of Annotations and Annotation Graphs"' +
				',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
			throw new StoreServiceException(200, json, "text/json", "UTF-8");
		} else if(annotationUris.size()>1) {
			// Annotation Set not found
			log.info("[" + apiKey + "] Multiple Annotation graphs detected... request rejected.");
			def json = JSON.parse('{"status":"nocontent" ,"message":"The request carries multiple Annotations or Annotation Graphs"' +
				',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
			throw new StoreServiceException(200, json, "text/json", "UTF-8");
		}
		
		String content;
		if(detectedAnnotationGraphsCounter>0) {
			// Query Specific Resources
			log.info("[" + apiKey + "] Identifiable Specific Resources detection...");
			int totalDetectedSpecificResources = 0;
			Set<Resource> specificResourcesUris = new HashSet<Resource>();
			Query  sparqlSpecificResources = QueryFactory.create("PREFIX oa: <http://www.w3.org/ns/oa#> SELECT DISTINCT ?s ?g WHERE " +
				"{{ GRAPH ?g { ?s a oa:SpecificResource . }}}");
			QueryExecution qSpecificResources  = QueryExecutionFactory.create (sparqlSpecificResources, dataset);
			ResultSet rSpecificResources = qSpecificResources.execSelect();
			while (rSpecificResources.hasNext()) {
				QuerySolution querySolution = rSpecificResources.nextSolution();
				specificResourcesUris.add(querySolution.get("s"));
				totalDetectedSpecificResources++;
			}
			log.info("[" + apiKey + "] Identifiable Specific Resources " + totalDetectedSpecificResources);
			
			// Query Embedded Resources
			log.info("[" + apiKey + "] Identifiable Content as Text detection...");
			int totalEmbeddedTextualBodies = 0;
			Set<Resource> embeddedTextualBodiesUris = new HashSet<Resource>();
			//Query  sparqlEmbeddedTextualBodies = QueryFactory.create("PREFIX cnt:<http://www.w3.org/2011/content#> SELECT DISTINCT ?s WHERE " +
			//"{{ GRAPH ?g { ?s a cnt:ContentAsText . }} UNION { ?s a cnt:ContentAsText . } FILTER (!isBlank(?s)) }");
			Query  sparqlEmbeddedTextualBodies = QueryFactory.create("PREFIX cnt:<http://www.w3.org/2011/content#> SELECT DISTINCT ?s ?g WHERE " +
				"{{ GRAPH ?g { ?s a cnt:ContentAsText . }} }");
			QueryExecution qEmbeddedTextualBodies  = QueryExecutionFactory.create (sparqlEmbeddedTextualBodies, dataset);
			ResultSet rEmbeddedTextualBodies = qEmbeddedTextualBodies.execSelect();
			while (rEmbeddedTextualBodies.hasNext()) {
				QuerySolution querySolution = rEmbeddedTextualBodies.nextSolution();
				embeddedTextualBodiesUris.add(querySolution.get("s"));
				totalEmbeddedTextualBodies++;
			}
			log.info("[" + apiKey + "] Identifiable Content as Text " + totalEmbeddedTextualBodies);
			
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			RDFDataMgr.write(outputStream, dataset, JenaJSONLD.JSONLD);
			content = outputStream.toString();
			content = identifiableURIs(apiKey, content, graphsUris, "graph")
			content = identifiableURIs(apiKey, content, annotationsGraphsUris, "graph")
			content = identifiableURIs(apiKey, content, annotationUris, "annotation")
			content = identifiableURIs(apiKey, content, specificResourcesUris, "resource")
			content = identifiableURIs(apiKey, content, embeddedTextualBodiesUris, "content")
		} else if(defaultGraphDetected) {
			// Do nothing
		} else {
			// Annotation Set not found
			log.info("[" + apiKey + "] Annotation not found " + content);
			def json = JSON.parse('{"status":"nocontent" ,"message":"The request does not carry acceptable payload or payload cannot be read"' +
				',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
			//render(status: 200, text: json, contentType: "text/json", encoding: "UTF-8");
			throw new StoreServiceException(200, json, "text/json", "UTF-8");
		}
		
		if(defaultGraphDetected) {
			Dataset workingDataset = dataset;
			//RDFDataMgr.read(workingDataset, new ByteArrayInputStream(content.toString().getBytes("UTF-8")), JenaJSONLD.JSONLD);
			
			identifiableURIs(apiKey, workingDataset.getDefaultModel(),
				ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
				ResourceFactory.createResource("http://www.w3.org/ns/oa#Annotation"), "annotation");
			
			identifiableURIs(apiKey, workingDataset.getDefaultModel(),
				ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
				ResourceFactory.createResource("http://www.w3.org/ns/oa#SpecificResource"), "resource");

			identifiableURIs(apiKey, workingDataset.getDefaultModel(),
				ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
				ResourceFactory.createResource("http://www.w3.org/2011/content#ContentAsText"), "content");
			
			Dataset dataset3 = DatasetFactory.createMem();
			dataset3.addNamedModel(getGraphUri(), workingDataset.getDefaultModel());
			
			virtuosoJenaStoreService.storeDataset(apiKey, dataset3);
			
			return dataset3
		} else {
			Dataset workingDataset = DatasetFactory.createMem();
			RDFDataMgr.read(workingDataset, new ByteArrayInputStream(content.toString().getBytes("UTF-8")), JenaJSONLD.JSONLD);
			
			return workingDataset;
		}
	}
	
	private String getGraphUri() {
		return 'http://' + grailsApplication.config.grails.server.host + ':' +
			grailsApplication.config.grails.server.port.http + '/s/graph/' +
			org.annotopia.grails.services.storage.utils.UUID.uuid();
	}
	
	private void identifiableURIs(String apiKey, Model model, Property property, Resource resource, String uriType) {
		
		log.info("[" + apiKey + "] Minting URIs " + uriType + " " + resource.toString());
		
		def uuid = org.annotopia.grails.services.storage.utils.UUID.uuid();
		def nUri = 'http://' + grailsApplication.config.grails.server.host + ':' +
						grailsApplication.config.grails.server.port.http + '/s/' + uriType + '/' + uuid;
		def rUri = ResourceFactory.createResource(nUri);

		def originalSubject;
		List<Statement> s = new ArrayList<Statement>();
		StmtIterator statements = model.listStatements(null, property, resource);
		println property.toString() + " - " + resource.toString();
		statements.each {
			originalSubject =  it.getSubject()
			StmtIterator statements2 = model.listStatements(it.getSubject(), null, null);
			statements2 .each { its ->
				s.add(model.createStatement(rUri, its.getPredicate(), its.getObject()));
			}
		}
		if(originalSubject==null) {
			log.info("[" + apiKey + "] Skipping URI " + uriType + " " + resource.toString());
			return;
		}
		model.removeAll(originalSubject, null, null);
	
		StmtIterator statements3 = model.listStatements(null, null, originalSubject);
		statements3.each { its2 ->
			s.add(model.createStatement(its2.getSubject(), its2.getPredicate(), rUri));
		}
		model.removeAll(null, null, originalSubject);
			
		s.each { model.add(it); }
		
		if(!originalSubject.isAnon()) 
			model.add(model.createStatement(rUri, 
				ResourceFactory.createProperty("http://purl.org/pav/previousVersion"), 
				ResourceFactory.createPlainLiteral(originalSubject.toString())
				));
		else 
			model.add(model.createStatement(rUri,
				ResourceFactory.createProperty("http://purl.org/pav/previousVersion"),
				ResourceFactory.createPlainLiteral("blank")
				));
	}
	
}
