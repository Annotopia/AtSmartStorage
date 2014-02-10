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

import virtuoso.jena.driver.VirtGraph
import virtuoso.jena.driver.VirtuosoQueryExecution
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory

import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.query.ResultSet
import com.hp.hpl.jena.rdf.model.RDFNode

/**
 * This is the service that allows to manage annotation via Jena APIs.
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class AnnotationJenaStorageService {

	def grailsApplication
	def virtuosoJenaStoreService
	
	public listAnnotationSet(apiKey, max, offset, tgtUrl, tgtFgt, tgtExt, tgtIds) {
		log.info '[' + apiKey + '] List annotation sets' + 
			'max:' + max + 
			'offset:' + offset +
			'tgtUrl:' + tgtUrl +
			'tgtFgt:' + tgtFgt;
		retrieveAnnotationGraphs(apiKey, max, offset, tgtUrl, tgtFgt);
	}
	
	public int countAnnotationGraphs(apiKey) {
		VirtGraph set = new VirtGraph (
			grailsApplication.config.annotopia.storage.triplestore.host,
			grailsApplication.config.annotopia.storage.triplestore.user,
			grailsApplication.config.annotopia.storage.triplestore.pass);
		
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
			"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation }}";
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
	
	public List<String> retrieveAnnotationGraphsNames(apiKey, max, offset, tgtUrl, tgtFgt) {
		log.info  '[' + apiKey + '] Retrieving annotation graphs names ' + 
			'max:' + max + 
			'offset:' + offset +
			'tgtUrl:' + tgtUrl +
			'tgtFgt:' + tgtFgt;
		
		VirtGraph set = new VirtGraph (
			grailsApplication.config.annotopia.storage.triplestore.host,
			grailsApplication.config.annotopia.storage.triplestore.user,
			grailsApplication.config.annotopia.storage.triplestore.pass);
		
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
			"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a oa:Annotation }} LIMIT " + max;
		
		if(tgtUrl!=null && tgtFgt=="false") {
			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
				"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a oa:Annotation . ?s oa:hasTarget <" + tgtUrl + "> }} LIMIT " + max;
		} else if(tgtUrl!=null && tgtFgt=="true") {
			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
				"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a oa:Annotation . {?s oa:hasTarget <" + tgtUrl + "> } UNION {?s oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource <" + tgtUrl + ">}}} LIMIT " + max;
		}
			
		log.trace('[' + apiKey + '] ' + queryString);
		
		List<String> graphs = new ArrayList<String>();
		Query  sparql = QueryFactory.create(queryString);
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (sparql, set);
		
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
	
	public Dataset retrieveAnnotationGraphs(apiKey, max, offset, tgtUrl, tgtFgt) {
		log.info '[' + apiKey + '] Retrieving annotation graphs';
	
		Dataset graphs;
		List<String> graphNames = retrieveAnnotationGraphsNames(apiKey, max, offset, tgtUrl, tgtFgt);
		graphNames.each { graphName ->
			if(graphs==null) graphs =  virtuosoJenaStoreService.retrieveGraph(apiKey, graphName);
			else {
				Dataset ds = virtuosoJenaStoreService.retrieveGraph(apiKey, graphName);
				ds.listNames().each { name ->
					graphs.addNamedModel(name, ds.getNamedModel(name));
				}
			}
		}
		return graphs;
	}	
	
	public Dataset retrieveAnnotationGraph(apiKey, uri) {
		log.info '[' + apiKey + '] Retrieving annotation graph';
	
		Dataset graphs;
		graphs =  virtuosoJenaStoreService.retrieveGraph(apiKey, uri);
		return graphs;
	}
}
