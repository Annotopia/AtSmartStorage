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
package org.annotopia.groovy.service.store

import java.util.Map;

import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.rdf.model.Model

/**
 * This is the interface that lists all the methods available for
 * Annotopia to communicate with the Triple Store of choice. This
 * interface is generic and does not assume the use of a specific
 * API.
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
interface ITripleStore {

	/**
	 * Stores the triples/quads loaded from a file.
	 * @param apiKey	The API key of the system requesting the transaction
	 * @param content Triples/quads in a file.
	 * @return
	 */
	public String store(String apiKey, File content);

	/**
	 * Stores the triples/quads loaded from a file.
	 * @param apiKey	The API key of the system requesting the transaction
	 * @param content Triples/quads in a file.
	 * @param baseUri Base URI (defaults to uri).
	 * @return
	 */
	public String store(String apiKey, File content, String baseUri);

	/**
	 * Stores the triples/quads loaded through a String.
	 * @param apiKey	The API key of the system requesting the transaction
	 * @param content Triples/quads in a String.
	 * @return
	 */
	public String store(String apiKey, String content);

	/**
	 * Stores the triples/quads loaded through a String
	 * @param apiKey	The API key of the system requesting the transaction
	 * @param content Triples/quads in a String.
	 * @param baseUri Base URI (defaults to uri).
	 * @return
	 */
	public String store(String apiKey, String content, String baseUri);

	/**
	 * Updates the triples/quads loaded from a file.
	 * @param apiKey	The API key of the system requesting the transaction
	 * @param content Triples/quads in a file.
	 * @return
	 */
	public String update(String apiKey, File content);

	/**
	 * Updates the triples/quads loaded from a file.
	 * @param apiKey	The API key of the system requesting the transaction
	 * @param content Triples/quads in a file.
	 * @param baseUri Base URI (defaults to uri).
	 * @return
	 */
	public String update(String apiKey, File content, String baseUri);

	/**
	 * Updates the triples/quads loaded through a String.
	 * @param apiKey	The API key of the system requesting the transaction
	 * @param content Triples/quads in a String.
	 * @return
	 */
	public String update(String apiKey, String content);

	/**
	 * Updates the triples/quads loaded through a String
	 * @param apiKey	The API key of the system requesting the transaction
	 * @param content Triples/quads in a String.
	 * @param baseUri Base URI (defaults to uri).
	 * @return
	 */
	public String update(String apiKey, String content, String baseUri);

	/**
	 * Retrieves the graph identified by the given URI
	 * @param apiKey	The API key of the system requesting the transaction
	 * @param graphUri	The URI identifying the graph to retrieve
	 */
	public Dataset retrieveGraph(String apiKey, String graphUri);

	/**
	 * Retrieves the metadata about the graph identified by the given URI
	 * @param apiKey	The API key of the system requesting the transaction
	 * @param graphUri	The URI identifying the graph to retrieve the metadata about
	 * @param metadataGraphUri	The URI identifying the graph containing all graphs metadata
	 */
	public Model retrieveGraphMetadata(String apiKey, String graphUri, String metadataGraphUri);

	public Model retrieveGraphIdentifiersMetadata(String apiKey, Map<String,String> identifiers, String metadataGraphUri);

	/**
	 * Verifies existence of the graph identified by the given URI
	 * @param apiKey	The API key of the system requesting the transaction
	 * @param graphUri  The URI identifying the graph
	 * @return True if the graph exists
	 */
	public boolean doesGraphExists(String apiKey, String graphUri);

	/**
	 * Drops the graph identified by the given URI
	 * @param apiKey	The API key of the system requesting the transaction
	 * @param graphUri	The URI identifying the graph to drop
	 * @return True if the graph has been dropped
	 */
	public boolean dropGraph(String apiKey, String graphUri);

	/**
	 * Clears the graph identified by the given URI
	 * @param apiKey	The API key of the system requesting the transaction
	 * @param graphUri	The URI identifying the graph to drop
	 * @return True if the graph has been dropped
	 */
	public boolean clearGraph(String apiKey, String graphUri);

	/**
	 * Remove all triples with the given object URI.
	 * @param apiKey	The API key of the system requesting the transaction
	 * @param objectUri	The URI identifying the object
	 */
	public void removeAllTriplesWithObject(String apiKey, String objectUri);
}
