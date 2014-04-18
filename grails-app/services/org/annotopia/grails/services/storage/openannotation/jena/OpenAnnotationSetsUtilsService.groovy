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
package org.annotopia.grails.services.storage.openannotation.jena

import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.QueryExecution
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.ResultSet
import com.hp.hpl.jena.rdf.model.Resource

/**
 * This is providing utilities for querying in memory data according to the Annotopia
 * Annotation Sets extensions over Open Annotation.
 * 
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class OpenAnnotationSetsUtilsService {

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
}
