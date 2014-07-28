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
	
	/**
	 * Counts all the graph (annotations) containing annotation meeting
	 * the given criteria.
	 * @param apiKey	The API key of the client issuing the request
	 * @param tgtUrls	The list of URLs identifying the targets of interest.
	 *                  If null all the available annotations will be returned.
	 *                  If empty none will be returned.
	 * @param tgtFgt	If true the results will include annotation of document fragments.
	 * 					If false, only the annotations on full resources will be returned.
	 * @return The total number of annotations meeting the given criteria.
	 */
	public int countAnnotationGraphs(apiKey, List<String> tgtUrls, tgtFgt, motivations) {
		log.info('[' + apiKey + '] Counting Annotation Graphs');
		StringBuffer queryBuffer = new StringBuffer();
		if(tgtUrls==null) { // Return any annotation
			// If the tgtFgt is not true we need to filter out the
			// annotations that target fragments
			if(tgtFgt!="true") {
				queryBuffer.append("FILTER NOT EXISTS { ?s oa:hasTarget ?sr. ?sr a oa:SpecificResource .}");
			}
		} else {
			// No results
			if(tgtUrls.size()==0) return 0;
			else {
				boolean first = false;
				tgtUrls.each { tgtUrl ->
					if(first) queryBuffer.append(" UNION ");
					if(tgtFgt=="false")
						queryBuffer.append("{ ?s oa:hasTarget <" + tgtUrl + "> }");
					else if(tgtFgt=="true")
						queryBuffer.append("{ ?s oa:hasTarget <" + tgtUrl + "> } UNION {?s oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource <" + tgtUrl + ">}");
					first=true;
				}
			}
		}
		
		if(motivations!=null && motivations.size()>0) {
			boolean first = false;
			motivations.each { motivation ->
				if(first) queryBuffer.append(" UNION ");
				if(motivation!='unmotivated') queryBuffer.append("{ ?s oa:motivatedBy ?motivation. FILTER (str(?motivation) = 'http://www.w3.org/ns/oa#" + motivation + "') }")
				else queryBuffer.append("{ ?s a oa:Annotation . FILTER NOT EXISTS { ?s oa:motivatedBy ?m. }}");
				first=true;
			}
		}
		
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
			"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation. " +
				queryBuffer.toString() +
			"}}";
		
		log.info('[' + apiKey + '] Query total accessible Annotation Graphs: ' + queryString);
		int totalCount = jenaVirtuosoStoreService.count(apiKey, queryString);
		log.info('[' + apiKey + '] Total accessible Annotation Graphs: ' + totalCount);
		totalCount;
	}
	
	/**
	 * Retrieves all the graph names of the graphs containing annotation meeting
	 * the given criteria.
	 * @param apiKey	The API key of the client issuing the request
	 * @param max		The maximum number of results to return (pagination)
	 * @param offset	The offset for the results (pagination)
	 * @param tgtUrls	The list of URLs identifying the targets of interest.
	 *                  If null all the available annotations will be returned.
	 *                  If empty none will be returned.
	 * @param tgtFgt	If true the results will include annotation of document fragments.
	 * 					If false, only the annotations on full resources will be returned.
	 * @return The graph names of the annotations meeting the given criteria.
	 */
	public Set<String> retrieveAnnotationGraphsNames(apiKey, max, offset, List<String> tgtUrls, tgtFgt, motivations) {
		log.info  '[' + apiKey + '] Retrieving annotation graphs names ' +
			' max:' + max +
			' offset:' + offset +
			' tgtUrls:' + tgtUrls +
			' tgtFgt:' + tgtFgt;
			
		StringBuffer queryBuffer = new StringBuffer();
		if(tgtUrls==null) { // Return any annotation
			// If the tgtFgt is not true we need to filter out the
			// annotations that target fragments
			if(tgtFgt!="true") {
				queryBuffer.append("FILTER NOT EXISTS { ?s oa:hasTarget ?sr. ?sr a oa:SpecificResource .}");
			}
		} else {
			if(tgtUrls.size()==0) return new HashSet<String>();
			// Returns annotations on the requested URLs (full resource)
			else {
				boolean first = false;
				tgtUrls.each { tgtUrl ->
					if(first) queryBuffer.append(" UNION ");
					if(tgtUrls.size()>0 && tgtFgt=="false")
						queryBuffer.append("{ ?s a oa:Annotation . ?s oa:hasTarget <" + tgtUrl + "> }");
					else if(tgtUrls.size()>0 && tgtFgt=="true")
						queryBuffer.append("{?s oa:hasTarget <" + tgtUrl + "> } UNION {?s oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource <" + tgtUrl + ">}")
					first=true;
				}
			}
		}
		
		if(motivations!=null && motivations.size()>0) {
			boolean first = false;
			motivations.each { motivation ->
				if(first) queryBuffer.append(" UNION ");
				if(motivation!='unmotivated') queryBuffer.append("{ ?s oa:motivatedBy ?motivation. FILTER (str(?motivation) = 'http://www.w3.org/ns/oa#" + motivation + "') }")
				else queryBuffer.append("{ ?s a oa:Annotation . FILTER NOT EXISTS { ?s oa:motivatedBy ?motivation. }}");
				first=true;
			}
		}
	
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
		"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a oa:Annotation. " +
			queryBuffer.toString() +
		"}} LIMIT " + max + " OFFSET " + offset;
		println '@@@@@@@ ' + queryString
		Set<String> graphs = jenaVirtuosoStoreService.retrieveGraphsNames(apiKey, queryString);
		graphs
	}
	
	/**
	 * Retrieves all the graphs containing a given annotation identified
	 * by the URI.
	 * @param apiKey	The API key of the client issuing the request
	 * @param uri		The identifier for the annotation
	 * @return The list of graphs containing the requested annotation.
	 */
	public Set<String> retrieveAnnotationGraphNames(apiKey, uri) {
		log.info '[' + apiKey + '] Retrieving Annotation Graph Names ' + uri;
		
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
			"SELECT DISTINCT ?g WHERE { GRAPH ?g { <" + uri + "> a oa:Annotation }}";
		
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
	
	// TODO From here on to be refactored
	
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
//	
//
//		
//	public int countAnnotationGraphs(apiKey, tgtUrl, tgtFgt, Map<String,String> identifiers) {
//		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
//			"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation }}";		
//		if(tgtUrl!=null && tgtFgt=="false") {
//			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
//				"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation . ?s oa:hasTarget <" + tgtUrl + "> }}";
//		} else if(tgtUrl!=null && tgtFgt=="true") {
//			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
//				"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation .  {?s oa:hasTarget <" + tgtUrl +
//				"> } UNION {?s oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource <" + tgtUrl + ">}}}";
//		} else if(tgtUrl==null && identifiers!=null && identifiers.size()>0) {
//			boolean first = false;
//			StringBuffer queryBuffer = new StringBuffer();
//			List<String> urls = jenaVirtuosoStoreService.retrieveAllManifestationsByIdentifiers(apiKey, identifiers, grailsApplication.config.annotopia.storage.uri.graph.identifiers);
//			println urls
//			urls.each { url ->
//				queryBuffer.append("{?s oa:hasTarget <" + url + "> } UNION {?s oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource <" + url + ">}")
//				if(first) queryBuffer.append(" UNION ");
//				first = true;
//			}
//			
//			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
//				"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation . " + queryBuffer.toString() + "}}";
//			println queryString
//		}
//			
//		int totalCount = jenaVirtuosoStoreService.count(apiKey, queryString);
//		log.info('[' + apiKey + '] Total accessible Annotation Graphs: ' + totalCount);
//		totalCount;
//	}
	
//	public int countAnnotationSetGraphs(apiKey, tgtUrl, tgtFgt) {
//		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
//			"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a <http://purl.org/annotopia#AnnotationSet> }}";
//		if(tgtUrl!=null && tgtFgt=="false") {
//			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
//				"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a <http://purl.org/annotopia#AnnotationSet> . ?s <http://purl.org/annotopia#annotations> ?a. ?a oa:hasTarget <" + tgtUrl + "> }}";
//		} else if(tgtUrl!=null && tgtFgt=="true") {
//			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
//				"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a <http://purl.org/annotopia#AnnotationSet> . ?s <http://purl.org/annotopia#annotations> ?a. " +
//					"{ ?a oa:hasTarget <" + tgtUrl + "> }" +
//					" UNION {?a <http://www.w3.org/ns/oa#hasTarget> ?t. ?t <http://www.w3.org/ns/oa#hasSource> <" + tgtUrl + "> }}}"
//				"}";
//		}
//			
//		int totalCount = jenaVirtuosoStoreService.count(apiKey, queryString);
//		log.info('[' + apiKey + '] Total accessible Annotation Set Graphs: ' + totalCount);
//		totalCount;
//	}

//	/**
//	 * Weather or not Annotation have been originally stored as graph, 
//	 * Annotopia wraps each anotation in a Named Graph. Therefore when
//	 * counting the total of annotations, the SPARQL query includes the
//	 * named graph parameter ?g
//	 * @param apiKey	The API key for the requerst
//	 * @return The total number of annotations that can be accessed.
//	 */
//	public int countAnnotations(apiKey) {
//
//		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
//			"SELECT (COUNT(DISTINCT ?s) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation }}";
//			
//		int totalCount = jenaVirtuosoStoreService.count(apiKey, queryString);
//		log.info('[' + apiKey + '] Total accessible Annotations in Named Graphs: ' + totalCount);
//		totalCount;
//	}


	
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
	
//	public Set<String> retrieveAnnotationSetsGraphsNames(apiKey, max, offset, tgtUrl, tgtFgt) {
//		log.info  '[' + apiKey + '] Retrieving annotation sets graphs names ' +
//			' max:' + max +
//			' offset:' + offset +
//			' tgtUrl:' + tgtUrl;
//			
//		String queryString = "PREFIX at: <http://purl.org/annotopia#> " +
//			"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a at:AnnotationSet }} LIMIT " + max + " OFFSET " + offset;
//		if(tgtUrl!=null) {
//			queryString = "PREFIX oa: <http://www.w3.org/ns/oa#> PREFIX at:  <http://purl.org/annotopia#> " +
//				"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a at:AnnotationSet . ?s <http://purl.org/annotopia#annotations> ?a. " +
//				"{ ?a oa:hasTarget <" + tgtUrl + "> }" +
//				" UNION {?a <http://www.w3.org/ns/oa#hasTarget> ?t. ?t <http://www.w3.org/ns/oa#hasSource> <" + tgtUrl + "> }}} LIMIT " + max + " OFFSET " + offset;
//		}
//		
//		Set<String> graphs = jenaVirtuosoStoreService.retrieveGraphsNames(apiKey, queryString);
//		graphs
//	}
	

	
//	public Dataset retrieveAnnotationSet(apiKey, uri) {
//		log.info '[' + apiKey + '] Retrieving annotation sets ' + uri;
//		
//		String queryString = "PREFIX at:  <http://purl.org/annotopia#> " +
//			"SELECT DISTINCT ?g WHERE { GRAPH ?g { <" + uri + "> a at:AnnotationSet }}";
//		log.trace('[' + apiKey + '] ' + queryString);
//	
//		VirtGraph graph = new VirtGraph (
//			grailsApplication.config.annotopia.storage.triplestore.host,
//			grailsApplication.config.annotopia.storage.triplestore.user,
//			grailsApplication.config.annotopia.storage.triplestore.pass);
//		
//		Dataset graphs;	
//		try {
//			VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (QueryFactory.create(queryString), graph);
//			ResultSet results = vqe.execSelect();
//			while (results.hasNext()) {
//				QuerySolution result = results.nextSolution();
//				RDFNode graph_name = result.get("g");
//				if(graphs==null) graphs =  jenaVirtuosoStoreService.retrieveGraph(apiKey, graph_name.toString());
//				else {
//					Dataset ds = jenaVirtuosoStoreService.retrieveGraph(apiKey, graph_name.toString());
//					ds.listNames().each { name ->
//						graphs.addNamedModel(name, ds.getNamedModel(name));
//					}
//					if(ds.getDefaultModel()!=null) {
//						println 'def model'
//						graphs.setDefaultModel(ds.getDefaultModel());
//					}
//				}
//			}
//			return graphs;
//		} catch (Exception e) {
//			log.error(e.getMessage());
//			return null;
//		}
//	}
}
