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

import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.query.ResultSet

/**
 * This service provides some basic statistics on the current open annotation 
 * content in the triple store.
 *
 * See also: http://code.google.com/p/void-impl/wiki/SPARQLQueriesForStatistics
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class OpenAnnotationReportingService {

	def grailsApplication;
	def jenaVirtuosoStoreService;
	
	private final PREFIX_OPEN_ANNOTATION 	= "PREFIX oa: <http://www.w3.org/ns/oa#> ";
	private final PREFIX_ANNOTOPIA 			= "PREFIX at: <http://purl.org/annotopia#> ";
	
	/**
	 * Counts all the Annotation items no matter if they are organized in sets or not.
	 * @param apiKey The apiKey used for the request.
	 * @return The number of Annotation items
	 */
	def countAnnotations(def apiKey) {
		String queryString = PREFIX_OPEN_ANNOTATION +
		"SELECT (COUNT(DISTINCT ?s) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation }}";
		jenaVirtuosoStoreService.count(apiKey, queryString);
	}
	
	/**
	 * Counts all the Annotation Sets.
	 * @param apiKey The apiKey used for the request
	 * @return The number of Annotation Sets
	 */
	def countAnnotationSets(def apiKey) {
		String queryString = PREFIX_ANNOTOPIA +
		"SELECT (COUNT(DISTINCT ?s) AS ?total) WHERE { GRAPH ?g { ?s a at:AnnotationSet }}";
		jenaVirtuosoStoreService.count(apiKey, queryString);
	}
	
	/**
	 * Counts all the resources that have been annotated. No matter if they
	 * have been annotated in full or not.
	 * @param apiKey The apiKey used for the request
	 * @return The number of annotated resources
	 */
	def countAnnotatedResources(def apiKey) {
		String queryString = PREFIX_OPEN_ANNOTATION + PREFIX_ANNOTOPIA +
			"SELECT (COUNT(DISTINCT ?u) AS ?total) WHERE { GRAPH ?g { ?a a oa:Annotation . { ?a oa:hasTarget ?u. FILTER NOT EXISTS { ?u  a oa:SpecificResource } } " +
			"UNION { ?a oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource ?u.}}}"
		jenaVirtuosoStoreService.count(apiKey, queryString);
	}
	
	/**
	 * Counts all the resources that have been annotated in full. Resources
	 * that have been annotated as fragments are not factored in.
	 * @param apiKey The apiKey used for the request
	 * @return The number of annotated (in-full) resources
	 */
	def countAnnotatedInFullResources(def apiKey) {
		String queryString = PREFIX_OPEN_ANNOTATION + PREFIX_ANNOTOPIA +
		"SELECT (COUNT(DISTINCT ?u) AS ?total) WHERE { GRAPH ?g { ?a a oa:Annotation . { ?a oa:hasTarget ?u. FILTER NOT EXISTS { ?u  a oa:SpecificResource } } " +
		"UNION { ?a oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource ?u.}}}"
		jenaVirtuosoStoreService.count(apiKey, queryString);
	}
	
	/**
	 * Counts all the resources that have been annotated in fragment. Resources
	 * that have been annotated in full are not factored in.
	 * @param apiKey The apiKey used for the request
	 * @return The number of annotated (in-full) resources
	 */
	def countAnnotatedInPartResources(def apiKey) {
		String queryString = PREFIX_OPEN_ANNOTATION + PREFIX_ANNOTOPIA +
		"SELECT (COUNT(DISTINCT ?u) AS ?total) WHERE { GRAPH ?g { ?a a oa:Annotation . { ?a oa:hasTarget ?u. FILTER NOT EXISTS { ?u  a oa:SpecificResource } } " +
		"UNION { ?a oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource ?u.}}}"
		jenaVirtuosoStoreService.count(apiKey, queryString);
	}

	/**
	 * Counts the annotations for each resource. No matter if they
	 * have been annotated in full or not.
	 * @param apiKey The apiKey used for the request
	 * @return A Map with all the resources and the counters of related annotations.
	 */
	Map<String, Integer> countAnnotationsForAllResources(def apiKey) {
		String queryString = PREFIX_OPEN_ANNOTATION + PREFIX_ANNOTOPIA +
		"SELECT ?target (COUNT(DISTINCT ?annotation) AS ?total) WHERE { GRAPH ?g { ?annotation a oa:Annotation . { ?a oa:hasTarget ?target. FILTER NOT EXISTS { ?target a oa:SpecificResource } } " +
		"UNION { ?a oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource ?target.}}} GROUP BY ?target";
		
		jenaVirtuosoStoreService.countAndGroupBy(apiKey, queryString, "total", "target");
	}
	
	/**
	 * Counts the annotations for each user (anntatedBy). 
	 * @param apiKey The apiKey used for the request
	 * @return A Map with all the users and the counters of related annotations.
	 */
	Map<String, Integer> countAnnotationsForEachUser(def apiKey) {
		String queryString = PREFIX_OPEN_ANNOTATION + PREFIX_ANNOTOPIA +
		"SELECT ?user (COUNT(DISTINCT ?annotation) AS ?total) WHERE { GRAPH ?g { ?annotation a oa:Annotation . { ?a oa:annotatedBy ?user }}} GROUP BY ?user";
		
		jenaVirtuosoStoreService.countAndGroupBy(apiKey, queryString, "total", "user");
	}
}
