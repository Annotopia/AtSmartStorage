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

import org.annotopia.grails.vocabularies.Bibliographic

import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.QueryExecution
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.query.ResultSet
import com.hp.hpl.jena.rdf.model.RDFNode
import com.hp.hpl.jena.rdf.model.Resource

/**
 * This service provides some utilities for analyzing Open Annotation content
 * when stored in memory.
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class OpenAnnotationUtilsService {
	
	/**
	 * Verifies if there are Annotation items in the default graph and returns the total number.
	 * @param apiKey			The API key of the client that issued the request
	 * @param dataset			The dataset containing the annotations
	 * @param annotationUris	The set of annotationUris
	 * @param closure			This closure is executed after the query.
	 * @return The total number of detected annotations in the default graph
	 */
	public int detectAnnotationsInDefaultGraph(apiKey, Dataset dataset, Set<Resource> annotationUris, Closure closure) {
		log.info("[" + apiKey + "] Detection of Annotation in Default Graph...");
		
		String QUERY = "PREFIX oa: <http://www.w3.org/ns/oa#> SELECT DISTINCT ?s WHERE { ?s a oa:Annotation . }"
		log.trace("[" + apiKey + "] Query: " + QUERY);
		
		int annotationsInDefaultGraphsCounter = 0;	
		QueryExecution queryExecution  = QueryExecutionFactory.create (QueryFactory.create(QUERY), dataset);
		ResultSet rAnnotationsInDefaultGraph = queryExecution.execSelect();
		while (rAnnotationsInDefaultGraph.hasNext()) {
			Resource annUri = rAnnotationsInDefaultGraph.nextSolution().getResource("s");
			annotationsInDefaultGraphsCounter++;
			annotationUris.add(annUri);
			
			// Alters the triples with the Annotation as subject
			if(closure!=null)  closure(dataset.getDefaultModel(), annUri);
		}
		
		if(annotationsInDefaultGraphsCounter>0) log.info("[" + apiKey + "] Annotation in Default Graph detected " +
			annotationsInDefaultGraphsCounter);
		else log.info("[" + apiKey + "] No Annotation in Default Graph detected");
		annotationsInDefaultGraphsCounter
	}
	
	/**
	 * Detects all the named graphs containing an Annotation instance.
	 * @param apiKey				The API key of the client that issued the request
	 * @param dataset				The dataset containing the annotations
	 * @param graphsUris			The full set of URIs of named graphs in the dataset
	 * @param annotationsGraphsUris The full set of Annotation URIs in Graphs belonging to the dataset
	 * @param annotationUris		The set of annotationUris
	 * @param closure				This closure is executed after the query.
	 * @return The total number of detected Annotations in Named Graphs
	 */
	public int detectAnnotationsInNamedGraph(apiKey, Dataset dataset, Set<Resource> graphsUris, 
			Set<Resource> annotationsGraphsUris, Set<Resource> annotationUris, Closure closure) {
		log.info("[" + apiKey + "] Detection of Annotation in Named Graphs...");
		
		String QUERY = "PREFIX oa: <http://www.w3.org/ns/oa#> SELECT ?s ?g WHERE { GRAPH ?g { ?s a oa:Annotation . }}";
		log.trace("[" + apiKey + "] Query: " + QUERY);
		
		int annotationGraphsCounter = 0;
		QueryExecution qAnnotationGraphs = QueryExecutionFactory.create (QueryFactory.create(QUERY), dataset);
		ResultSet rAnnotationGraphs = qAnnotationGraphs.execSelect();
		while (rAnnotationGraphs.hasNext()) {
			QuerySolution querySolution = rAnnotationGraphs.nextSolution();
			
			Resource graphUri = querySolution.getResource("g");
			annotationsGraphsUris.add(graphUri);
			graphsUris.remove(graphUri);
			
			Resource annUri = querySolution.getResource("s");
			annotationUris.add(annUri);
			
			// Add saving data
			if(closure!=null)   closure(dataset.getNamedModel(graphUri.toString()), annUri);

			annotationGraphsCounter++;
		}
		log.info("[" + apiKey + "] Annotation graphs " + annotationGraphsCounter);
		annotationGraphsCounter
	}
			
	/**
	 * Detects all the Specific Resources that are defined in Named Graphs.
	 * @param apiKey				The API key of the client that issued the request
	 * @param dataset				The dataset containing the annotations
	 * @param specificResourcesUris The full set of Specific Resources URIs in Graphs belonging to the dataset
	 * @return The total number of detected Specific Resources in Named Graphs.
	 */
	public int detectSpecificResourcesAsNamedGraphs(apiKey, Dataset dataset, Set<Resource> specificResourcesUris) {
		log.info("[" + apiKey + "] Specific Resources in Named Graphs detection...");
		
		String QUERY = "PREFIX oa: <http://www.w3.org/ns/oa#> SELECT DISTINCT ?s WHERE " +
			"{{ GRAPH ?g { ?s a oa:SpecificResource . }}}";
		log.trace("[" + apiKey + "] Query: " + QUERY);
		
		int specificResourcesGraphsCounter = 0;
		QueryExecution qSpecificResources  = QueryExecutionFactory.create (QueryFactory.create(QUERY), dataset);
		ResultSet rSpecificResources = qSpecificResources.execSelect();
		while (rSpecificResources.hasNext()) {
			QuerySolution querySolution = rSpecificResources.nextSolution();
			specificResourcesUris.add(querySolution.get("s"));
			specificResourcesGraphsCounter++;
		}
		log.info("[" + apiKey + "] Identifiable Specific Resources as Named Graphs " + specificResourcesGraphsCounter);
		specificResourcesGraphsCounter
	}
	
	public int detectContextAsTextInDefaultGraph(apiKey, Dataset dataset, Set<Resource> embeddedTextualBodiesUris) {
		log.info("[" + apiKey + "] Identifiable Content as Text detection in default graph...");
		String QUERY = "PREFIX cnt:<http://www.w3.org/2011/content#> SELECT DISTINCT ?s WHERE " +
			"{{ { ?s a cnt:ContentAsText . }} }"
		
		int embeddedTextualBodiesCounter = 0;
		QueryExecution qEmbeddedTextualBodies  = QueryExecutionFactory.create (QueryFactory.create(QUERY), dataset);
		ResultSet rEmbeddedTextualBodies = qEmbeddedTextualBodies.execSelect();
		while (rEmbeddedTextualBodies.hasNext()) {
			QuerySolution querySolution = rEmbeddedTextualBodies.nextSolution();
			embeddedTextualBodiesUris.add(querySolution.get("s"));
			embeddedTextualBodiesCounter++;
		}
		log.info("[" + apiKey + "] Identifiable Content as Text " + embeddedTextualBodiesCounter);
		embeddedTextualBodiesCounter
	}
	
	/**
	 * Detects all the Content As Text instances defined in Named Graphs
	 * @param apiKey					The API key of the client that issued the request
	 * @param dataset					The dataset containing the annotations
	 * @param embeddedTextualBodiesUris The full set of Content As Text URIs in Graphs belonging to the dataset
	 * @return The total number of detected Content As Text in Named Graphs.
	 */
	public int detectContextAsTextInNamedGraphs(apiKey, Dataset dataset, Set<Resource> embeddedTextualBodiesUris) {
		log.info("[" + apiKey + "] Identifiable Content as Text detection...");
		String QUERY = "PREFIX cnt:<http://www.w3.org/2011/content#> SELECT DISTINCT ?s WHERE " +
			"{{ GRAPH ?g { ?s a cnt:ContentAsText . }} }"
		
		int embeddedTextualBodiesCounter = 0;
		QueryExecution qEmbeddedTextualBodies  = QueryExecutionFactory.create (QueryFactory.create(QUERY), dataset);
		ResultSet rEmbeddedTextualBodies = qEmbeddedTextualBodies.execSelect();
		while (rEmbeddedTextualBodies.hasNext()) {
			QuerySolution querySolution = rEmbeddedTextualBodies.nextSolution();
			embeddedTextualBodiesUris.add(querySolution.get("s"));
			embeddedTextualBodiesCounter++;
		}
		log.info("[" + apiKey + "] Identifiable Content as Text " + embeddedTextualBodiesCounter);
		embeddedTextualBodiesCounter
	}
	
	public int detectBodiesAsNamedGraphs(apiKey, Dataset dataset, Set<Resource> bodiesGraphsUris) {
		log.info("[" + apiKey + "] Bodies in Named Graphs detection...");
		String QUERY = "PREFIX oa: <http://www.w3.org/ns/oa#> SELECT DISTINCT ?s ?graph WHERE " +
			"{GRAPH ?g { ?s oa:hasBody ?graph .} GRAPH ?graph {?a ?b ?c .}}"
		
		int bodiesGraphsCounter = 0;
		QueryExecution gBodiesAsGraphs  = QueryExecutionFactory.create (QueryFactory.create(QUERY), dataset);
		ResultSet rBodiesAsGraphs = gBodiesAsGraphs.execSelect();
		while (rBodiesAsGraphs.hasNext()) {
			QuerySolution querySolution = rBodiesAsGraphs.nextSolution();
			bodiesGraphsUris.add(querySolution.get("graph"));
			bodiesGraphsCounter++;
		}
		log.info("[" + apiKey + "] Identifiable Bodies as Named Graphs " + bodiesGraphsCounter);
		bodiesGraphsCounter
	}
	
	public Map<String,String> detectTargetIdentifiersInDefaultGraph(apiKey, Dataset dataset, Map<String,String> identifiers) {
		log.info("[" + apiKey + "] Target identifiers detection...");
		String QUERY = "PREFIX frbr: <http://purl.org/vocab/frbr/core#> " +
			"PREFIX prism: <http://prismstandard.org/namespaces/basic/2.0/> " +
			"PREFIX oa: <http://www.w3.org/ns/oa#> " +
			"PREFIX fabio:<http://purl.org/spar/fabio#> " +
			"SELECT DISTINCT ?doi ?pmid ?pmcid ?pii ?target WHERE { " +
			"{{ ?ann oa:hasTarget ?target . ?target frbr:embodimentOf ?s. OPTIONAL { ?s prism:doi ?doi .} OPTIONAL {?s fabio:hasPubMedId ?pmid .} OPTIONAL { ?s fabio:hasPII ?pii .} OPTIONAL { ?s fabio:hasPubMedCentralId ?pmcid . }} " +
			"UNION { ?ann oa:hasTarget ?spt. ?spt oa:hasSource ?target. ?target frbr:embodimentOf ?s. OPTIONAL {?s prism:doi ?doi .} OPTIONAL {?s fabio:hasPubMedId ?pmid .} OPTIONAL {?s fabio:hasPII ?pii .} OPTIONAL {?s fabio:hasPubMedCentralId ?pmcid .} }}}"

		QueryExecution gIdentifiers  = QueryExecutionFactory.create (QueryFactory.create(QUERY), dataset);
		ResultSet rIdentifiers = gIdentifiers.execSelect();
		while (rIdentifiers.hasNext()) {
			QuerySolution querySolution = rIdentifiers.nextSolution();
	
			RDFNode url = querySolution.get("target");
			if(url!=null) {
				identifiers.put(Bibliographic.LABEL_URL, url.toString());
			
				RDFNode doi = querySolution.get(Bibliographic.LABEL_DOI);
				if(doi!=null) identifiers.put(Bibliographic.LABEL_DOI, doi.toString());
				
				RDFNode pmid = querySolution.get(Bibliographic.LABEL_PMID);
				if(pmid!=null) identifiers.put(Bibliographic.LABEL_PMID, pmid.toString());
				
				RDFNode pmcid = querySolution.get(Bibliographic.LABEL_PMCID);
				if(pmcid!=null) identifiers.put(Bibliographic.LABEL_PMCID, pmcid.toString());
				
				RDFNode pii = querySolution.get(Bibliographic.LABEL_PII);
				if(pii!=null) identifiers.put(Bibliographic.LABEL_PII, pii.toString());
			}
		}
	}
	
	public Map<String,String> detectTargetIdentifiers(apiKey, Dataset dataset, Map<String,String> identifiers) {
		log.info("[" + apiKey + "] Target identifiers detection...");
		String QUERY = "PREFIX frbr: <http://purl.org/vocab/frbr/core#> " + 
			"PREFIX prism: <http://prismstandard.org/namespaces/basic/2.0/> " + 
			"PREFIX oa: <http://www.w3.org/ns/oa#> " + 
			"PREFIX fabio:<http://purl.org/spar/fabio#> " + 
			"SELECT DISTINCT ?doi ?pmid ?pmcid ?pii ?target WHERE {GRAPH ?g " + 
			"{{ ?ann oa:hasTarget ?target . ?target frbr:embodimentOf ?s. ?s prism:doi ?doi . ?s fabio:hasPubMedId ?pmid . ?s fabio:hasPII ?pii . ?s fabio:hasPubMedCentralId ?pmcid . } " + 
			"UNION { ?ann oa:hasTarget ?spt. ?spt oa:hasSource ?target. ?target frbr:embodimentOf ?s. ?s prism:doi ?doi . ?s fabio:hasPubMedId ?pmid . ?s fabio:hasPII ?pii . ?s fabio:hasPubMedCentralId ?pmcid . }}}"

		QueryExecution gIdentifiers  = QueryExecutionFactory.create (QueryFactory.create(QUERY), dataset);
		ResultSet rIdentifiers = gIdentifiers.execSelect();
		while (rIdentifiers.hasNext()) {
			QuerySolution querySolution = rIdentifiers.nextSolution();
	
			RDFNode url = querySolution.get("target");
			if(url!=null) {
				identifiers.put("url", url.toString());
			
				RDFNode doi = querySolution.get(Bibliographic.LABEL_DOI);
				if(doi!=null) identifiers.put(Bibliographic.LABEL_DOI, doi.toString());
				
				RDFNode pmid = querySolution.get(Bibliographic.LABEL_PMID);
				if(pmid!=null) identifiers.put(Bibliographic.LABEL_PMID, pmid.toString());
				
				RDFNode pmcid = querySolution.get(Bibliographic.LABEL_PMCID);
				if(pmcid!=null) identifiers.put(Bibliographic.LABEL_PMCID, pmcid.toString());
				
				RDFNode pii = querySolution.get(Bibliographic.LABEL_PII);
				if(pii!=null) identifiers.put(Bibliographic.LABEL_PII, pii.toString());
			}
		}
	}
}
