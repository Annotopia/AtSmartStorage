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

import java.util.List;

import virtuoso.jena.driver.VirtGraph
import virtuoso.jena.driver.VirtuosoQueryExecution
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query
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
	
	public int countAnnotationGraphs(apiKey, tgtUrl, tgtFgt) {
		VirtGraph graph = new VirtGraph (
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
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (QueryFactory.create(queryString), graph);
		ResultSet results=vqe.execSelect();
		if(results.hasNext()) {
			totalCount = Integer.parseInt(results.next().get("total").getString());
		}
		log.info('[' + apiKey + '] Total accessible Annotation Graphs: ' + totalCount);
		totalCount;
	}
	
	public int countAnnotations(apiKey) {
		VirtGraph graph = new VirtGraph (
			grailsApplication.config.annotopia.storage.triplestore.host,
			grailsApplication.config.annotopia.storage.triplestore.user,
			grailsApplication.config.annotopia.storage.triplestore.pass);
		
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
			"SELECT (COUNT(DISTINCT ?s) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation }}";
		log.trace('[' + apiKey + ']' +  queryString);
			
		int totalCount = 0;
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (QueryFactory.create(queryString), graph);
		ResultSet results=vqe.execSelect();
		if(results.hasNext())
		{
			totalCount = Integer.parseInt(results.next().get("total").getString());
		}
		log.info('[' + apiKey + '] Total accessible Annotations in Named Graphs: ' + totalCount);
		totalCount;
	}
	
	public Set<String> retrieveAnnotationGraphNames(apiKey, uri) {
		
		log.info '[' + apiKey + '] Retrieving Annotation Graph Names ' + uri;
		
		VirtGraph graph = new VirtGraph (
			grailsApplication.config.annotopia.storage.triplestore.host,
			grailsApplication.config.annotopia.storage.triplestore.user,
			grailsApplication.config.annotopia.storage.triplestore.pass);
		
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
			"SELECT DISTINCT ?g WHERE { GRAPH ?g { <" + uri + "> a oa:Annotation }}";	
		log.trace('[' + apiKey + '] ' + queryString);
		
		Set<String> graphs = new HashSet<String>();
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (QueryFactory.create(queryString), graph);	
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
	
	public List<String> retrieveAnnotationGraphsNames(apiKey, max, offset, tgtUrl, tgtFgt) {
		log.info  '[' + apiKey + '] Retrieving annotation graphs names ' +
			' max:' + max +
			' offset:' + offset +
			' tgtUrl:' + tgtUrl +
			' tgtFgt:' + tgtFgt;
		
		VirtGraph graph = new VirtGraph (
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
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (QueryFactory.create(queryString), graph);
		try {
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
	
	public Dataset retrieveAnnotation(apiKey, uri) {
		log.info '[' + apiKey + '] Retrieving annotation ' + uri;
	
		VirtGraph graph = new VirtGraph (
			grailsApplication.config.annotopia.storage.triplestore.host,
			grailsApplication.config.annotopia.storage.triplestore.user,
			grailsApplication.config.annotopia.storage.triplestore.pass);
		
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
			"SELECT DISTINCT ?g WHERE { GRAPH ?g { <" + uri + "> a oa:Annotation }}";		
		log.trace('[' + apiKey + '] ' + queryString);
		
		Dataset graphs;
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (QueryFactory.create(queryString), graph);	
		try {
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
