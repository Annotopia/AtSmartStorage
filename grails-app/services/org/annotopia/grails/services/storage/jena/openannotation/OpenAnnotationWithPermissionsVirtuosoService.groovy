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
class OpenAnnotationWithPermissionsVirtuosoService {

	def grailsApplication
	def usersService;
	def groupsService;
	def jenaVirtuosoStoreService
	
	/**
	 * Injects the SPARQL query with a fragments that enable text search on the
	 * specified documents including or excluding terget fragments.
	 * @param queryBuffer	The buffer for constructing the query
	 * @param tgtUrls		List of targets allowed in search results
	 * @param tgtFgt		If true, target fragments will be included in search
	 * @return  The query with or without the target filters.
	 */
	private boolean getTargetFilter(queryBuffer, tgtUrls, tgtFgt) {
		if(tgtUrls==null) { // Return any annotation
			// If the tgtFgt is not true we need to filter out the
			// annotations that target fragments
			if(tgtFgt!="true") {
				queryBuffer.append("FILTER NOT EXISTS { ?s oa:hasTarget ?sr. ?sr a oa:SpecificResource .}");
			}
		} else {
			if(tgtUrls.size()==0) return false;
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
		return true;
	}
	
	/**
	 * Injects the SPARQL query with a fragments that enable text search on the
	 * title of the bibliographic data of the annotated resource.
	 * @param queryBuffer	The buffer for constructing the query
	 * @param text			The text to search
	 * @return The query with or without the new fragment for searching the title.
	 */
	private getTargetTitleFilter(queryBuffer, text) {
		if(text==null) {
			return false;
		} else {
			//queryBuffer.append("{?s oa:hasTarget ?t2. ?t2 dct:title ?title1 FILTER regex(str(?title1), \""+ text + "\", \"i\")} UNION {?s oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource ?s1. ?s1 dct:title ?title2 FILTER regex(str(?title2), \""+ text + "\", \"i\")}")
			queryBuffer.append("{?s oa:hasTarget ?t2. ?t2 dct:title ?title1 FILTER contains(?title1, \""+ text + "\")} UNION {?s oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource ?s1. ?s1 dct:title ?title2 FILTER contains(?title2, \""+ text + "\")}")
		}
	}
	
	/**
	 * Injects the SPARQL query with a fragments that enable the search only
	 * for annotation with the specified source.
	 * @param queryBuffer	The buffer for constructing the query
	 * @param sources		The list of allowed sources (usually clients)
	 * @return The query with or without the new fragment for filtering on sources.
	 */
	private getSourcesFilter(queryBuffer, sources) {
		if(sources!=null && sources.size()>0) {
			boolean first = false;
			sources.each { source ->
				if(first) queryBuffer.append(" UNION ");
				//if(source!='others' && source!='unspecified') queryBuffer.append("{ ?s oa:serializedBy ?software. FILTER (str(?software) = 'urn:application:" + source + "') }")
				if(source!='others' && source!='unspecified') queryBuffer.append("{ ?s oa:serializedBy <urn:application:" + source + "> }")
				else if(source=='other') {}
				else queryBuffer.append("{ ?s a oa:Annotation . FILTER NOT EXISTS { ?s oa:serializedBy ?m. }}");
				first=true;
			}
		}
	}
	
	/**
	 * Injects the SPARQL query with a fragments that enable the search only
	 * for annotation with the specified motivations.
	 * @param queryBuffer	The buffer for constructing the query
	 * @param motivations	The list of allowed motivations
	 * @return The query with or without the new fragment for filtering on motivations.
	 */
	private getMotivationsFilter(queryBuffer, motivations) {
		if(motivations!=null && motivations.size()>0) {
			boolean first = false;
			motivations.each { motivation ->
				if(first) queryBuffer.append(" UNION ");
				//if(motivation!='unmotivated') queryBuffer.append("{ ?s oa:motivatedBy ?motivation. FILTER (str(?motivation) = 'http://www.w3.org/ns/oa#" + motivation + "') }")
				if(motivation!='unmotivated') queryBuffer.append("{ ?s oa:motivatedBy <http://www.w3.org/ns/oa#" + motivation + "> }")
				else queryBuffer.append("{ ?s a oa:Annotation . FILTER NOT EXISTS { ?s oa:motivatedBy ?m. }}");
				first=true;
			}
		}
	}
	
	// --------------------------------------------------------
	//  TEXT SEARCH
	// --------------------------------------------------------
	/**
	 * Injects the SPARQL query with a fragments that enable text search 
	 * @param queryBuffer	The buffer for constructing the query
	 * @param text			The text to search for
	 * @param motivations	The list of allowed motivations
	 * @param inclusions	The list of fields included in the search.
	 * @return The query including the fragments for text search.
	 */
	private getTextSearchFilter(queryBuffer, text, motivations, inclusions) {
		if(text!=null && text.length()>0) {
			queryBuffer.append("{");
			getEmbeddedTextualBodyTextFilter(queryBuffer, text, motivations);
			if(inclusions.contains("document title")) {
				queryBuffer.append(" UNION ");
				getTargetTitleFilter(queryBuffer, text);
			}
			queryBuffer.append(" UNION ");
			getSemanticTagTextFilter(queryBuffer, text, motivations);
			getHighligthTextFilter(queryBuffer, text, motivations);
			queryBuffer.append("}");
		}
	}
	
	private void getEmbeddedTextualBodyTextFilter(queryBuffer, text, motivations) {
		//queryBuffer.append("{ ?s oa:hasBody ?b1. ?b1 cnt:chars ?content. FILTER regex(?content, \"" + text + "\", \"i\")  }");
		queryBuffer.append("{ ?s oa:hasBody ?b1. ?b1 cnt:chars ?content. FILTER contains(UCASE(str(?content)), \"" + text.toUpperCase() + "\")  }");
	}
	
	private void getSemanticTagTextFilter(queryBuffer, text, motivations) {
		//queryBuffer.append("{ ?s oa:hasBody ?b2. ?b2 rdfs:label ?content. FILTER regex(?content, \"" + text + "\", \"i\")  }");
		queryBuffer.append("{ ?s oa:hasBody ?b2. ?b2 rdfs:label ?content. FILTER contains(UCASE(str(?content)), \"" + text.toUpperCase() + "\")  }");
	}
	
	private void getHighligthTextFilter(queryBuffer, text, motivations) {
		if(motivations!=null && motivations.size()>0 && ("highlighting" in motivations)) {
			queryBuffer.append(" UNION ");
			//queryBuffer.append("{ ?s oa:motivatedBy ?motivation1. FILTER (str(?motivation1) = 'http://www.w3.org/ns/oa#highlighting'). ?s oa:hasTarget ?t1. ?t1 oa:hasSelector ?selector. ?selector a oa:TextQuoteSelector. ?selector oa:exact ?exact. FILTER regex(?exact, \"" + text + "\", \"i\")  }");
			queryBuffer.append("{ ?s oa:motivatedBy <http://www.w3.org/ns/oa#highlighting>. ?s oa:hasTarget ?t1. ?t1 oa:hasSelector ?selector. ?selector a oa:TextQuoteSelector. ?selector oa:exact ?exact. FILTER contains(?exact, \"" + text + "\")  }");
		}
	}

	// --------------------------------------------------------
	//  PERMISSION FILTER
	// --------------------------------------------------------
	/**
	 * Injects the SPARQL query with a fragments that defines the 
	 * set of permission criteria for the annotation to search.
	 * @param queryBuffer	The buffer for constructing the query
	 * @param userKey
	 * @return
	 */
	private void getReadPermissionQueryChunk(queryBuffer, def permissions, def user) {
		
		boolean first = false;
		//def buffer = "{?x <http://purl.org/annotopia#read> ?agent . FILTER (str(?agent)=\"" + user.id + "\")}";
		StringBuffer sb = new StringBuffer();
		sb.append("{");
		
		// User identifiers
		//
		
		if(permissions.contains("private")) {
			sb.append("{?x <http://purl.org/annotopia#read> ?agent . FILTER (str(?agent)=\"user:" + user.id + "\")}");
			def userIds = usersService.getUserAgentIdentifiers(user.id);
			if(userIds!=null && userIds.size()>0) {
				userIds.eachWithIndex{ userId, index ->
					sb.append(" UNION ")
					//sb.append("{?x <http://purl.org/annotopia#read> ?agent . FILTER (str(?agent)=\"user:" + userId + "\")}");
					sb.append("{?x <http://purl.org/annotopia#read> ?agent . FILTER (contains(str(?agent), \"user:" + userId + "\"))}");
					first = true;
				}
			}
		}

		// Groups identifiers
		if(permissions.contains("groups")) {
			def groups = groupsService.listUsersGroups(user);
			if(groups!=null && groups.size()>0) {		
				groups.eachWithIndex{ group, index ->
					def groupIdentifiers = groupsService.getGroupAgentIdentifiers(group.id);
					groupIdentifiers.each{ groupId ->
						if(first) sb.append(" UNION ");
						sb.append("{?x <http://purl.org/annotopia#read> ?agent . FILTER (str(?agent)=\"group:" + groupId + "\")}");
						first = true;
					}				
				}
				
			}	
		}
		
		// Public option
		if(permissions.contains("public")) {
			if(first) sb.append(" UNION ");
			sb.append("{?s <http://purl.org/annotopia#permissions> ?x . FILTER NOT EXISTS {?x <http://purl.org/annotopia#read> ?agent .} }");
			first = true;
		}
		
		// Public option
		if(permissions.contains("unspecified")) {
			if(first) sb.append(" UNION ");
			sb.append("{?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> oa:Annotation . FILTER NOT EXISTS {?s <http://purl.org/annotopia#permissions> ?x .} }");
		}
		
		sb.append("}");
		queryBuffer.append(sb.toString());
	}
	
	/**
	 * Count for search services. Counts all the available results that meet
	 * the specified criteria (including facets).
	 * @param apiKey		The system api key
	 * @param user			The user that performed the request
	 * @param tgtUrls		The target urls if any
	 * @param tgtFgt		If to include fragments or not
	 * @param text			The text to search
	 * @param permissions	The permissions facet values
	 * @param sources		The sources facet values (originator system)
	 * @param motivations	The motivations facet values
	 * @param inclusions	The inclusions facet values
	 * @return The count of the available annotations meeting the psecified criteria.
	 */
	public int countAnnotationGraphs(apiKey, user, List<String> tgtUrls, tgtFgt, text, permissions, sources, motivations, inclusions) {
		log.debug('[' + apiKey + '] Counting total search accessible Annotation Graphs');
		long start = System.currentTimeMillis();
		
		StringBuffer queryBuffer = new StringBuffer();
		if(!getTargetFilter(queryBuffer, tgtUrls, tgtFgt)) return 0;
		getReadPermissionQueryChunk(queryBuffer, permissions, user);
		getSourcesFilter(queryBuffer, sources);
		getMotivationsFilter(queryBuffer, motivations);
		getTextSearchFilter(queryBuffer, text, motivations, inclusions);
		
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> PREFIX  cnt: <http://www.w3.org/2011/content#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  PREFIX dct:  <http://purl.org/dc/terms/> " +
			"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation. " +
				queryBuffer.toString() +
			"}}";
		
		log.trace('[' + apiKey + '] Query total accessible Annotation Graphs: ' + queryString);
		int totalCount = jenaVirtuosoStoreService.count(apiKey, queryString);
		log.debug('[' + apiKey + '] Total search accessible Annotation Graphs: ' + totalCount);
		log.trace('[' + apiKey + '] TIME DURATION (countAnnotationGraphs): ' + (System.currentTimeMillis()-start));
		totalCount;	
	}
	
	/**
	 * Returns the names of all the graphs meeting the search criteria.
	 * @param apiKey		The system api key
	 * @param user			The user that performed the request
	 * @param userIds		All the known IDs for the requesting user
	 * @param max			The max number of results (pagination)
	 * @param offset		The results offset (pagination)
	 * @param tgtUrls		The target urls if an
	 * @param tgtFgt		If to include fragments or not
	 * @param text			The text to search
	 * @param permissions	The permissions facet values
	 * @param sources		The sources facet values (originator system)
	 * @param motivations	The motivations facet values
	 * @param inclusions	The inclusions facet values
	 * @return 
	 */
	public Set<String> retrieveAnnotationGraphsNames(apiKey, user, userIds, max, offset, tgtUrls, tgtFgt, text, permissions, sources, motivations, inclusions) {
		log.debug('[' + apiKey + '] Retrieving annotation graphs names ' +
			' max:' + max + ' offset:' + offset + ' tgtUrl:' + tgtUrls + ' tgtFgt:' + tgtFgt);
		long start = System.currentTimeMillis();
		
		StringBuffer queryBuffer = new StringBuffer();
		if(!getTargetFilter(queryBuffer, tgtUrls, tgtFgt)) return 0;
		getReadPermissionQueryChunk(queryBuffer, permissions, user);
		getSourcesFilter(queryBuffer, sources);
		getMotivationsFilter(queryBuffer, motivations);
		getTextSearchFilter(queryBuffer, text, motivations, inclusions);
			
		String queryString = "PREFIX oa: <http://www.w3.org/ns/oa#> PREFIX  cnt: <http://www.w3.org/2011/content#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  PREFIX dct:  <http://purl.org/dc/terms/> " +
		"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a oa:Annotation. " +
			queryBuffer.toString() +
		"}}";

		log.trace('[' + apiKey + '] Query Annotation Graphs: ' + queryString);
		Set<String> graphs = jenaVirtuosoStoreService.retrieveGraphsNames(apiKey, queryString);
		log.trace('[' + apiKey + '] TIME DURATION (retrieveAnnotationGraphsNames): ' + (System.currentTimeMillis()-start));
		graphs
	}
	

	public int countAnnotationGraphs(apiKey, user, tgtUrl, tgtFgt, permissions, motivations) {

		log.debug('[' + apiKey + '] Counting total accessible Annotation Graphs');
		long start = System.currentTimeMillis();
		
		StringBuffer queryBuffer = new StringBuffer();
		if(!getTargetFilter(queryBuffer, tgtUrl, tgtFgt)) return 0;
		getReadPermissionQueryChunk(queryBuffer, permissions, user);
		getMotivationsFilter(queryBuffer, motivations);
		
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> PREFIX  cnt: <http://www.w3.org/2011/content#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  PREFIX dct:  <http://purl.org/dc/terms/> " +
			"SELECT (COUNT(DISTINCT ?g) AS ?total) WHERE { GRAPH ?g { ?s a oa:Annotation. " +
				queryBuffer.toString() +
			"}}";
		
		log.trace('[' + apiKey + '] Query total accessible Annotation Graphs: ' + queryString);
		int totalCount = jenaVirtuosoStoreService.count(apiKey, queryString);
		log.debug('[' + apiKey + '] Total search accessible Annotation Graphs: ' + totalCount);
		log.trace('[' + apiKey + '] TIME DURATION (countAnnotationGraphs): ' + (System.currentTimeMillis()-start));
		totalCount;
	}
	
	public Set<String> retrieveAnnotationGraphsNames(apiKey, user, userIds, max, offset, tgtUrls, tgtFgt, permissions, motivations, inclusions) {
		log.debug('[' + apiKey + '] Retrieving annotation graphs names ' +
			' max:' + max + ' offset:' + offset + ' tgtUrl:' + tgtUrls + ' tgtFgt:' + tgtFgt);
		long start = System.currentTimeMillis();
		
		StringBuffer queryBuffer = new StringBuffer();
		if(!getTargetFilter(queryBuffer, tgtUrls, tgtFgt)) return 0;
		getReadPermissionQueryChunk(queryBuffer, permissions, user);
		getMotivationsFilter(queryBuffer, motivations);
			
		String queryString = "PREFIX oa: <http://www.w3.org/ns/oa#> PREFIX  cnt: <http://www.w3.org/2011/content#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  PREFIX dct:  <http://purl.org/dc/terms/> " +
		"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a oa:Annotation. " +
			queryBuffer.toString() +
		"}}";

		log.trace('[' + apiKey + '] Query Annotation Graphs: ' + queryString);
		Set<String> graphs = jenaVirtuosoStoreService.retrieveGraphsNames(apiKey, queryString);
		log.trace('[' + apiKey + '] TIME DURATION (retrieveAnnotationGraphsNames): ' + (System.currentTimeMillis()-start));
		graphs
	}
	
	public Set<String> retrieveAnnotationGraphsNames(apiKey, user, userIds, max, offset, tgtUrl, tgtFgt, permissions, motivations) {
		log.info  '[' + apiKey + '] Retrieving annotation graphs names ' +
			' max:' + max +
			' offset:' + offset +
			' tgtUrl:' + tgtUrl +
			' tgtFgt:' + tgtFgt;
		long start = System.currentTimeMillis();
		
		StringBuffer queryBuffer = new StringBuffer();
		if(!getTargetFilter(queryBuffer, tgtUrl, tgtFgt)) return 0;
		getReadPermissionQueryChunk(queryBuffer, permissions, user);
		getMotivationsFilter(queryBuffer, motivations);
			
		String queryString = "PREFIX oa: <http://www.w3.org/ns/oa#> PREFIX  cnt: <http://www.w3.org/2011/content#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  PREFIX dct:  <http://purl.org/dc/terms/> " +
		"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a oa:Annotation. " +
			queryBuffer.toString() +
		"}}";

		log.trace('[' + apiKey + '] Query Annotation Graphs: ' + queryString);
		Set<String> graphs = jenaVirtuosoStoreService.retrieveGraphsNames(apiKey, queryString);
		log.trace('[' + apiKey + '] TIME DURATION (retrieveAnnotationGraphsNames): ' + (System.currentTimeMillis()-start));
		graphs
			/*
		def buffer = getReadPermissionQueryChunk(userKey);
			
//		def buffer = "?x <http://purl.org/annotopia#read> <" + userKey + ">.";
//		StringBuffer sb = new StringBuffer();
//		if(userIds!=null && userIds.size()>0) {
//			sb.append("{");
//			sb.append("?x <http://purl.org/annotopia#read> <" + userKey + ">.")
//			sb.append("} UNION {")
//			userIds.eachWithIndex{ userId, index ->
//				sb.append("?x <http://purl.org/annotopia#read> <user:" + userId + ">.")
//				if(index<userIds.size()-1) sb.append(" UNION ");
//			}
//			sb.append("}");
//			buffer = sb.toString();
//		}

		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
			"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a oa:Annotation. " + buffer + " }} LIMIT " + max + " OFFSET " + offset;

		if(tgtUrl!=null && tgtFgt=="false") {
			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
				"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a oa:Annotation . ?s oa:hasTarget <" + tgtUrl + ">. " + buffer + "  }} LIMIT " + max + " OFFSET " + offset;
		} else if(tgtUrl!=null && tgtFgt=="true") {
			queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
				"SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s a oa:Annotation . {?s oa:hasTarget <" + tgtUrl +
				"> } UNION {?s oa:hasTarget ?t. ?t a oa:SpecificResource. ?t oa:hasSource <" + tgtUrl + ">.}  " + buffer + " }} LIMIT " + max + " OFFSET " + offset;
		}
	
		Set<String> graphs = jenaVirtuosoStoreService.retrieveGraphsNames(apiKey, queryString);
		graphs
		*/
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
	
	public Set<String> whoCanUpdateAnnotation(apiKey, userKey, graphUri) {
		log.info '[' + apiKey + '] Checking if user ' + userKey + ' can update Annotation ' + graphUri;
		
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
			"SELECT DISTINCT ?allowed FROM <" + graphUri + "> WHERE {  { ?s a oa:Annotation. ?s ?p ?x. ?x <http://purl.org/annotopia#update> ?allowed. }}";

		Set<String> enabled = jenaVirtuosoStoreService.retrievePropertyValues(apiKey, queryString, "allowed");
		enabled
	}
	
	public Set<String> whoCanDeleteAnnotation(apiKey, userKey, graphUri) {
		log.info '[' + apiKey + '] Checking if user ' + userKey + ' can update Annotation ' + graphUri;
		
		String queryString = "PREFIX oa:   <http://www.w3.org/ns/oa#> " +
			"SELECT DISTINCT ?allowed FROM <" + graphUri + "> WHERE {  { ?s a oa:Annotation. ?s ?p ?x. ?x <http://purl.org/annotopia#delete> ?allowed. }}";

		Set<String> enabled = jenaVirtuosoStoreService.retrievePropertyValues(apiKey, queryString, "allowed");
		enabled
	}
	
	
	
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
	
	// TODO return all graphs also the onces that are bodies!!!!
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
