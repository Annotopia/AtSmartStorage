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
package org.annotopia.grails.services.storage.utils.jena

import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFLanguages

import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QueryExecution
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.query.ResultSet
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.Resource

/**
 * Basic abstract Jena triple store utilities.
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class JenaUtilsService {
	
	/**
	 * Retrieves the set of all Named Graphs
	 * @param apiKey			The API key of the client that issued the request
	 * @param dataset			The dataset containing the annotations
	 * @return The set of all Named Graphs
	 */
	public Set<Resource> detectNamedGraphs(apiKey, Dataset dataset) {
		log.info("[" + apiKey + "] Named Graphs detection...");
		Set<Resource> graphsUris = new HashSet<Resource>();
		Query  sparqlGraphs = QueryFactory.create("SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o . }}");
		QueryExecution qGraphs = QueryExecutionFactory.create (sparqlGraphs, dataset);
		ResultSet rGraphs = qGraphs.execSelect();
		while (rGraphs.hasNext()) {
			QuerySolution querySolution = rGraphs.nextSolution();
			graphsUris.add(querySolution.get("g"));
		}
		log.info("[" + apiKey + "] Named Graphs detected " + graphsUris.size());
		graphsUris
	}
	
	/**
	 * Returns a String representation in JSON-LD format of a Dataset
	 * @param dataset	The Dataset to serialize
	 * @return The serialization of the Dataset
	 */
	public String getDatasetAsString(Dataset dataset) {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		RDFDataMgr.write(outputStream, dataset, RDFLanguages.JSONLD);
		return outputStream.toString();
	}
	
	/**
	 * Returns a String representation in JSON-LD format of a Dataset
	 * @param dataset	The Dataset to serialize
	 * @return The serialization of the Dataset
	 */
	public String getDatasetAsString(Model model) {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		RDFDataMgr.write(outputStream, model, RDFLanguages.JSONLD);
		return outputStream.toString();
	}
}
