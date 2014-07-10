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

import virtuoso.jena.driver.VirtGraph
import virtuoso.jena.driver.VirtuosoQueryExecution
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory

import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.query.ResultSet
import com.hp.hpl.jena.rdf.model.RDFNode


/**
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class OpenAnnotationVirtuosoService {

	def grailsApplication
	def jenaVirtuosoStoreService
	
//	public int countAnnotationGraphs(apiKey,tgtFgt) {
//		// Retrieves all the annotations
//		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
//			"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation }}";
//		if(tgtFgt=="false") {
//			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
//				"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation }}";
//		}
//		
//		int totalCount = jenaVirtuosoStoreService.count(apiKey, queryString);
//		log.info('[' + apiKey + '] Total accessible Annotation Graphs: ' + totalCount);
//		totalCount;
//	}
	
	public int countAnnotationGraphs(apiKey, List<String> tgtUrls, tgtFgt) {
		if(tgtUrls!=null && tgtUrls.size()==0) return 0;
		
		// Retrieves all the annotations
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
			"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation }}";
		// Retrieves the annotations on an entire given set of document urls
		if(tgtUrls!=null && tgtUrls.size()>0 && tgtFgt=="false") {
			boolean first = false;
			StringBuffer queryBuffer = new StringBuffer();
			tgtUrls.each { tgtUrl ->
				if(first) queryBuffer.append(" UNION ");
				queryBuffer.append("{ ?s a oa:Annotation . ?s oa:hasTarget <" + tgtUrl + "> }");
				first=true;
			}
			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> SELECT (COUNT(DISTINCT ?g) AS ?total) " + 
				"WHERE { GRAPH ?g {" + 
					queryBuffer.toString() +
				"}}";
		// Retrieves the annotations on an entire given set of document urls or their fragments
		} else if(tgtUrls!=null && tgtUrls.size()>0 && tgtFgt=="true") {
			boolean first = false;
			StringBuffer queryBuffer = new StringBuffer();
			tgtUrls.each { tgtUrl ->
				if(first) queryBuffer.append(" UNION ");
				queryBuffer.append("{ ?s a oa:Annotation . ?s oa:hasTarget <" + tgtUrl + "> } UNION {?s oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource <" + tgtUrl + ">}");
				first=true;
			}
			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
				"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g {" +
					queryBuffer.toString() +
				"}}";
		} 
		
		int totalCount = jenaVirtuosoStoreService.count(apiKey, queryString);
		log.info('[' + apiKey + '] Total accessible Annotation Graphs: ' + totalCount);
		totalCount;
	}	
		
	public int countAnnotationGraphs(apiKey, tgtUrl, tgtFgt, Map<String,String> identifiers) {
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
			"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation }}";		
		if(tgtUrl!=null && tgtFgt=="false") {
			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
				"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation . ?s oa:hasTarget <" + tgtUrl + "> }}";
		} else if(tgtUrl!=null && tgtFgt=="true") {
			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
				"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation .  {?s oa:hasTarget <" + tgtUrl +
				"> } UNION {?s oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource <" + tgtUrl + ">}}}";
		} else if(tgtUrl==null && identifiers!=null && identifiers.size()>0) {
			boolean first = false;
			StringBuffer queryBuffer = new StringBuffer();
			List<String> urls = jenaVirtuosoStoreService.retrieveAllManifestationsByIdentifiers(apiKey, identifiers, grailsApplication.config.annotopia.storage.uri.graph.identifiers);
			println urls
			urls.each { url ->
				queryBuffer.append("{?s oa:hasTarget <" + url + "> } UNION {?s oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource <" + url + ">}")
				if(first) queryBuffer.append(" UNION ");
				first = true;
			}
			
			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
				"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation . " + queryBuffer.toString() + "}}";
			println queryString
		}
			
		int totalCount = jenaVirtuosoStoreService.count(apiKey, queryString);
		log.info('[' + apiKey + '] Total accessible Annotation Graphs: ' + totalCount);
		totalCount;
	}
	
	public int countAnnotationSetGraphs(apiKey, tgtUrl, tgtFgt) {
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
			"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a <http://purl.org/annotopia#AnnotationSet> }}";
		if(tgtUrl!=null && tgtFgt=="false") {
			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
				"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a <http://purl.org/annotopia#AnnotationSet> . ?s <http://purl.org/annotopia#annotations> ?a. ?a oa:hasTarget <" + tgtUrl + "> }}";
		} else if(tgtUrl!=null && tgtFgt=="true") {
			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
				"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a <http://purl.org/annotopia#AnnotationSet> . ?s <http://purl.org/annotopia#annotations> ?a. " +
					"{ ?a oa:hasTarget <" + tgtUrl + "> }" +
					" UNION {?a <http://www.w3.org/ns/oa#hasTarget> ?t. ?t <http://www.w3.org/ns/oa#hasSource> <" + tgtUrl + "> }}}"
				"}";
		}
			
		int totalCount = jenaVirtuosoStoreService.count(apiKey, queryString);
		log.info('[' + apiKey + '] Total accessible Annotation Set Graphs: ' + totalCount);
		totalCount;
	}

	/**
	 * Weather or not Annotation have been originally stored as graph, 
	 * Annotopia wraps each anotation in a Named Graph. Therefore when
	 * counting the total of annotations, the SPARQL query includes the
	 * named graph parameter ?g
	 * @param apiKey	The API key for the requerst
	 * @return The total number of annotations that can be accessed.
	 */
	public int countAnnotations(apiKey) {

		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
			"SELECT (COUNT(DISTINCT ?s) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation }}";
			
		int totalCount = jenaVirtuosoStoreService.count(apiKey, queryString);
		log.info('[' + apiKey + '] Total accessible Annotations in Named Graphs: ' + totalCount);
		totalCount;
	}

	public Set<String> retrieveAnnotationGraphNames(apiKey, uri) {
		
		log.info '[' + apiKey + '] Retrieving Annotation Graph Names ' + uri;
		
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
			"SELECT DISTINCT ?g WHERE { GRAPH ?g { <" + uri + "> a oa:Annotation }}";
		
		Set<String> graphs = jenaVirtuosoStoreService.retrieveGraphsNames(apiKey, queryString);
		graphs
	}
	
	public Set<String> retrieveAnnotationGraphsNames(apiKey, max, offset, List<String> tgtUrls, tgtFgt) {
		log.info  '[' + apiKey + '] Retrieving annotation graphs names ' +
			' max:' + max +
			' offset:' + offset +
			' tgtUrl:' + tgtUrls +
			' tgtFgt:' + tgtFgt;
			
		if(tgtUrls!=null && tgtUrls.size()==0) return new HashSet<String>();

		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
			"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a oa:Annotation }} LIMIT " + max + " OFFSET " + offset;
		if(tgtUrls!=null && tgtUrls.size()>0 && tgtFgt=="false") {
			boolean first = false;
			StringBuffer queryBuffer = new StringBuffer();
			tgtUrls.each { tgtUrl ->
				if(first) queryBuffer.append(" UNION ");
				queryBuffer.append("{ ?s a oa:Annotation . ?s oa:hasTarget <" + tgtUrl + "> }");
				first=true;
			}
			
			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
				"SELECT DISTINCT ?g WHERE { GRAPH ?g {" + 
					queryBuffer.toString() + 
				"} LIMIT " + max + " OFFSET " + offset;
		} else if(tgtUrls!=null && tgtUrls.size()>0 && tgtFgt=="true") {
			boolean first = false;
			StringBuffer queryBuffer = new StringBuffer();
			//List<String> urls = jenaVirtuosoStoreService.retrieveAllManifestationsByIdentifiers(apiKey, identifiers, grailsApplication.config.annotopia.storage.uri.graph.identifiers);
			//println urls
			tgtUrls.each { tgtUrl ->
				if(first) queryBuffer.append(" UNION ");
				queryBuffer.append("{?s oa:hasTarget <" + tgtUrl + "> } UNION {?s oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource <" + tgtUrl + ">}")			
				first = true;
			}
		
			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
				"SELECT DISTINCT ?g WHERE { GRAPH ?g {" + 
					queryBuffer.toString() +
				"}} LIMIT " + max + " OFFSET " + offset;
		}
	
		Set<String> graphs = jenaVirtuosoStoreService.retrieveGraphsNames(apiKey, queryString);
		graphs
	}
	
//	public Set<String> retrieveAnnotationGraphsNames(apiKey, max, offset, tgtUrl, tgtFgt) {
//		log.info  '[' + apiKey + '] Retrieving annotation graphs names ' +
//			' max:' + max +
//			' offset:' + offset +
//			' tgtUrl:' + tgtUrl +
//			' tgtFgt:' + tgtFgt;
//
//		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
//			"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a oa:Annotation }} LIMIT " + max + " OFFSET " + offset;
//		if(tgtUrl!=null && tgtFgt=="false") {
//			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
//				"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a oa:Annotation . ?s oa:hasTarget <" + tgtUrl + "> }} LIMIT " + max + " OFFSET " + offset;
//		} else if(tgtUrl!=null && tgtFgt=="true") {
//			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
//				"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a oa:Annotation . {?s oa:hasTarget <" + tgtUrl +
//				"> } UNION {?s oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource <" + tgtUrl + ">}}} LIMIT " + max + " OFFSET " + offset;
//		}
//	
//		Set<String> graphs = jenaVirtuosoStoreService.retrieveGraphsNames(apiKey, queryString);
//		graphs
//	}
	
	public Set<String> retrieveAnnotationSetsGraphsNames(apiKey, max, offset, tgtUrl, tgtFgt) {
		log.info  '[' + apiKey + '] Retrieving annotation sets graphs names ' +
			' max:' + max +
			' offset:' + offset +
			' tgtUrl:' + tgtUrl;
			
		String queryString = "PREFIX at: <http://purl.org/annotopia#> " +
			"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a at:AnnotationSet }} LIMIT " + max + " OFFSET " + offset;
		if(tgtUrl!=null) {
			queryString = "PREFIX oa: <http://www.w3.org/ns/oa#> PREFIX at:  <http://purl.org/annotopia#> " +
				"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a at:AnnotationSet . ?s <http://purl.org/annotopia#annotations> ?a. " +
				"{ ?a oa:hasTarget <" + tgtUrl + "> }" +
				" UNION {?a <http://www.w3.org/ns/oa#hasTarget> ?t. ?t <http://www.w3.org/ns/oa#hasSource> <" + tgtUrl + "> }}} LIMIT " + max + " OFFSET " + offset;
		}
		
		Set<String> graphs = jenaVirtuosoStoreService.retrieveGraphsNames(apiKey, queryString);
		graphs
	}
	
	// TODO return all graphs also the ones that are bodies!!!!
	public Dataset retrieveAnnotation(apiKey, uri) {
		log.info '[' + apiKey + '] Retrieving annotation ' + uri;
		
		String queryString = "PREFIX oa:<http://www.w3.org/ns/oa#> " +
			"SELECT ?body ?graph  WHERE {{ GRAPH ?graph { <" + uri + "> a oa:Annotation }} " +
			"UNION { GRAPH ?g {<" + uri + "> oa:hasBody ?body. ?body a <http://www.w3.org/2004/03/trix/rdfg-1/Graph>}}}";
		log.trace('[' + apiKey + '] ' + queryString);
	
		VirtGraph graph = new VirtGraph (
			grailsApplication.config.annotopia.storage.triplestore.host,
			grailsApplication.config.annotopia.storage.triplestore.user,
			grailsApplication.config.annotopia.storage.triplestore.pass);
		
		Dataset graphs;
		try {
			VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (QueryFactory.create(queryString), graph);
			ResultSet results = vqe.execSelect();
			while (results.hasNext()) {
				QuerySolution result = results.nextSolution();
				RDFNode graph_name = result.get("graph");
				RDFNode body_name = result.get("body");
				if(graph_name!=null) {
					if(graphs==null) graphs =  jenaVirtuosoStoreService.retrieveGraph(apiKey, graph_name.toString());
					else {
						Dataset ds = jenaVirtuosoStoreService.retrieveGraph(apiKey, graph_name.toString());
						ds.listNames().each { name ->
							graphs.addNamedModel(name, ds.getNamedModel(name));
						}
						if(ds.getDefaultModel()!=null) {
							graphs.setDefaultModel(ds.getDefaultModel());
						}
					}
				}
				if(body_name!=null) {
					if(graphs==null) graphs =  jenaVirtuosoStoreService.retrieveGraph(apiKey, body_name.toString());
					else {
						Dataset ds = jenaVirtuosoStoreService.retrieveGraph(apiKey, body_name.toString());
						ds.listNames().each { name ->
							graphs.addNamedModel(name, ds.getNamedModel(name));
						}
					}
				}
			}
			return graphs;
		} catch (Exception e) {
			log.error(e.getMessage());
			return null;
		}
	}
	
	public Dataset retrieveAnnotationSet(apiKey, uri) {
		log.info '[' + apiKey + '] Retrieving annotation sets ' + uri;
		
		String queryString = "PREFIX at:  <http://purl.org/annotopia#> " +
			"SELECT DISTINCT ?g WHERE { GRAPH ?g { <" + uri + "> a at:AnnotationSet }}";
		log.trace('[' + apiKey + '] ' + queryString);
	
		VirtGraph graph = new VirtGraph (
			grailsApplication.config.annotopia.storage.triplestore.host,
			grailsApplication.config.annotopia.storage.triplestore.user,
			grailsApplication.config.annotopia.storage.triplestore.pass);
		
		Dataset graphs;	
		try {
			VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (QueryFactory.create(queryString), graph);
			ResultSet results = vqe.execSelect();
			while (results.hasNext()) {
				QuerySolution result = results.nextSolution();
				RDFNode graph_name = result.get("g");
				if(graphs==null) graphs =  jenaVirtuosoStoreService.retrieveGraph(apiKey, graph_name.toString());
				else {
					Dataset ds = jenaVirtuosoStoreService.retrieveGraph(apiKey, graph_name.toString());
					ds.listNames().each { name ->
						graphs.addNamedModel(name, ds.getNamedModel(name));
					}
					if(ds.getDefaultModel()!=null) {
						println 'def model'
						graphs.setDefaultModel(ds.getDefaultModel());
					}
				}
			}
			return graphs;
		} catch (Exception e) {
			log.error(e.getMessage());
			return null;
		}
	}
}
