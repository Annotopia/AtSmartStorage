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
package org.annotopia.grails.controllers.annotation

import grails.converters.JSON

import java.text.SimpleDateFormat
import java.util.regex.Pattern

import javax.servlet.http.HttpServletRequest

import org.apache.jena.riot.RDFDataMgr

import com.github.jsonldjava.jena.JenaJSONLD
import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.DatasetFactory
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QueryExecution
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.QuerySolution
import com.hp.hpl.jena.query.ResultSet
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.Property
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.rdf.model.ResourceFactory
import com.hp.hpl.jena.rdf.model.Statement
import com.hp.hpl.jena.rdf.model.StmtIterator

/**
 * This is the REST API for managing Annotation.
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class AnnotationController {
	
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssz")

	def annotationJenaStorageService;
	def virtuosoJenaStoreService;
	
	// curl -i -X GET http://localhost:8080/s/annotation
	// curl -i -X GET http://localhost:8080/s/annotation --header "Content-Type: application/json" --data '{"apiKey":"testkey"}'
	// curl -i -X GET http://localhost:8080/s/annotation --header "Content-Type: application/json" --data '{"apiKey":"testkey" ,  "tgtUrl":"http://www.jbiomedsem.com/content/2/S2/S4"}'
	// curl -i -X GET http://localhost:8080/s/annotation --header "Content-Type: application/json" --data '{"apiKey":"testkey" ,  "tgtUrl":"http://www.jbiomedsem.com/content/2/S2/S4", "tgtFgt":"true"}'
	// curl -i -X GET http://localhost:8080/s/annotation --header "Content-Type: application/json" --data '{"apiKey":"testkey" ,  "tgtUrl":"http://www.jbiomedsem.com/content/2/S2/S4", "tgtFgt":"true", "max":"1"}'
	
	// curl -i -X GET http://localhost:8080/res/annotation/001 --header "Content-Type: application/json" --data '{"apiKey":"testkey"}'
	def show = {
		long startTime = System.currentTimeMillis();
		
		def apiKey = request.JSON.apiKey;
		boolean allowed = (
			grailsApplication.config.annotopia.storage.testing.enabled=='true' && 
			apiKey==grailsApplication.config.annotopia.storage.testing.apiKey
		);
		if(!allowed) {
			def json = JSON.parse('{"status":"rejected" ,"message":"Api Key missing or invalid"}');
			render(status: 401, text: json, contentType: "text/json", encoding: "UTF-8");
			return;
		}
		
		if(params.id==null) {
			// Pagination
			def max = (request.JSON.max!=null)?request.JSON.max:"10";
			def offset = (request.JSON.offset!=null)?request.JSON.offset:"0";
			
			// Target filters
			def tgtUrl = request.JSON.tgtUrl
			def tgtFgt = request.JSON.tgtFgt
			def tgtExt = request.JSON.tgtExt
			def tgtIds = request.JSON.tgtIds
			def flavor = request.JSON.flavor
			log.info("[" + apiKey + "] List >>" + 
				" max:" + max +
				" offset:" + offset +
				((tgtUrl!=null) ? (" tgtUrl:" + tgtUrl):"") +
				((tgtFgt!=null) ? (" tgtFgt:" + tgtFgt):"") +
				((tgtExt!=null) ? (" tgtExt:" + tgtExt):"") + 
				((tgtIds!=null) ? (" tgtIds:" + tgtIds):"") +
				((flavor!=null) ? (" flavor:" + flavor):""));
			
			def result = ']}'
	
			int total = annotationJenaStorageService.countAnnotationGraphs(apiKey, tgtUrl, tgtFgt);
			int pages = (total/Integer.parseInt(max));
			
			if(total>0 && Integer.parseInt(offset)>0 && Integer.parseInt(offset)>=pages) {
				log.info("[" + apiKey + "] The requested page " + offset + " does not exist, the page index limit is " + (pages==0?"0":(pages-1)) );
				def json = JSON.parse('{"status":"rejected" ,"message":"The requested page ' + offset + ' does not exist, the page index limit is ' + (pages==0?"0":(pages-1))+ '"' + 
					',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
				render(status: 401, text: json, contentType: "text/json", encoding: "UTF-8");
				return;
			}
			
			Dataset graphs = annotationJenaStorageService.listAnnotation(apiKey, max, offset, tgtUrl, tgtFgt, tgtExt, tgtIds);
	
			if(graphs!=null) {
				response.contentType = "text/json;charset=UTF-8"
				response.outputStream << '{"status":"results", "result": {' + 
					'"total":"' + total + '", ' +
					'"pages":"' + pages + '", ' +
					'"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' +
					'"offset": "' + offset + '", ' +
					'"max": "' + max + '", ' +
					'"items":['
						
				RDFDataMgr.write(response.outputStream, graphs, JenaJSONLD.JSONLD);
				
				response.outputStream <<  ']}}';
				response.outputStream.flush()
			} else { 
				// No Annotation Sets found with the specified criteria
				log.info("[" + apiKey + "] No Annotation found with the specified criteria");
				response.contentType = "text/json;charset=UTF-8"
				response.outputStream << '{"status":"nocontent","message":"No results with the chosen criteria" , "result": {' + 
					'"total":"' + total + '", ' +
					'"pages":"' + pages + '", ' +
					'"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' +
					'"offset": "' + offset + '", ' +
					'"max": "' + max + '", ' +
					'"items":['
				response.outputStream <<  ']}}';
				response.outputStream.flush()
			}
		} else {
			Dataset graphs =  annotationJenaStorageService.retrieveAnnotation(apiKey, getCurrentUrl(request));
			
			if(graphs.listNames().hasNext()) {
				response.contentType = "text/json;charset=UTF-8"

				RDFDataMgr.write(response.outputStream, graphs, JenaJSONLD.JSONLD);
				
				response.outputStream.flush()
			} else {
				// Annotation Set not found
				log.info("[" + apiKey + "] Graph not found");
				def json = JSON.parse('{"status":"notfound" ,"message":"The requested resource ' + getCurrentUrl(request) + ' has not been found"' +
					',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
				render(status: 404, text: json, contentType: "text/json", encoding: "UTF-8");
				return;
			}
		}
	}
	
	// curl -i -X POST http://localhost:8080/s/annotation
	// curl -i -X POST http://localhost:8080/s/annotation --header "Content-Type: application/json" --data '{"apiKey":"testkey"}'
	// curl -i -X POST http://localhost:8080/s/annotation --header "Content-Type: application/json" --data '{"apiKey":"testkey", "item":{"@context":"https://gist.github.com/hubgit/6105255/raw/36f89110f7cb28fb605f7722048167d82644f946/open-annotation-context.json" ,"@graph" : [ {"@id" : "urn:domeo:1","@type" : "http://www.w3.org/ns/oa#Annotation","http://www.w3.org/ns/oa#hasBody" : {"@id" : "http://www.example.org/body3"},"http://www.w3.org/ns/oa#hasTarget" : {"@id" : "http://www.example.org/target3"} } ],"@id" : "urn:domeo:2"}}'
	// curl -i -X POST http://localhost:8080/s/annotation --header "Content-Type: application/json" --data '{"apiKey":"testkey", "validate":"ON", "flavor":"OA", "item":{"@context":"https://gist.github.com/hubgit/6105255/raw/36f89110f7cb28fb605f7722048167d82644f946/open-annotation-context.json" ,"@graph" : [ {"@id" : "http://www.example.org/ann1","@type" : "http://www.w3.org/ns/oa#Annotation","http://www.w3.org/ns/oa#hasBody" : {"@id" : "http://www.example.org/body3"},"http://www.w3.org/ns/oa#hasTarget" : {"@id" : "http://www.example.org/target3"} } ],"@id" : "http://annotopiaserver.org/annotationset/92"}}'
	
	// curl -i -X POST http://localhost:8080/s/annotation --header "Content-Type: application/json" --data '{"apiKey":"testkey", "item":{"@context": "https://gist.github.com/hubgit/6105255/raw/36f89110f7cb28fb605f7722048167d82644f946/open-annotation-context.json","@graph": [{"@id": "urn:temp:3","@type": "rdf:Graph","@graph": {"@id": "urn:temp:4","@type": "oa:Annotation","hasBody": "urn:temp:5","hasTarget": "http://www.example.org/target1"}},{"@id": "urn:temp:5","@type": "rdf:Graph","@graph": {"@id": "http://www.example.org/artifact1","label": "yolo body"}}]}}'

	// Annotation and specific resource
	// curl -i -X POST http://localhost:8080/s/annotation --header "Content-Type: application/json" --data '{"apiKey":"testkey", "item":{"@context": "https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/OAContext.json","@graph": [{"@id": "urn:temp:5","@type": "rdf:Graph","@graph": {"@id": "http://www.example.org/artifact1","label": "yolo body"}},{"@id": "urn:temp:6","@type": "rdf:Graph","@graph": {"@id": "urn:temp:7","@type": "oa:Annotation","hasBody": "http://paolociccarese.info","hasTarget": {"@id": "urn:temp:8","@type": "oa:SpecificResource","hasSelector": {"@id": "urn:temp:9","@type": "oa:FragmentSelector","conformsTo": "http://www.w3.org/TR/media-frags/","value": "xywh=10,10,5,5"},"hasSource": {"@id": "http://www.example.org/images/logo.jpg","@type": "dctypes:Image"}}}}]}}'	
	// Multiple annotations
	// curl -i -X POST http://localhost:8080/s/annotation --header "Content-Type: application/json" --data '{"apiKey":"testkey", "item":{"@context": "https://gist.github.com/hubgit/6105255/raw/36f89110f7cb28fb605f7722048167d82644f946/open-annotation-context.json","@graph": [{"@id": "urn:temp:3","@type": "rdf:Graph","@graph": {"@id": "urn:temp:4","@type": "oa:Annotation","hasBody": "urn:temp:5","hasTarget": "http://www.example.org/target1"}},{"@id": "urn:temp:5","@type": "rdf:Graph","@graph": {"@id": "http://www.example.org/artifact1","label": "yolo body"}},{"@id": "urn:temp:6","@type": "rdf: Graph","@graph": {"@id": "urn:temp:7","@type": "oa:Annotation","hasBody": "http://paolociccarese.info","hasTarget": "http: //www.example.org/target1"}}]}}'
	
	// Multiple annotations and specific resource
	// curl -i -X POST http://localhost:8080/s/annotation --header "Content-Type: application/json" --data '{"apiKey":"testkey", "item":{"@context": "https://gist.github.com/hubgit/6105255/raw/36f89110f7cb28fb605f7722048167d82644f946/open-annotation-context.json","@graph": [{"@id": "urn:temp:3","@type": "rdf:Graph","@graph": {"@id": "urn:temp:4","@type": "oa:Annotation","hasBody": "urn:temp:5","hasTarget": "http://www.example.org/target1"}},{"@id": "urn:temp:5","@type": "rdf:Graph","@graph": {"@id": "http://www.example.org/artifact1","label": "yolo body"}},{"@id": "urn:temp:6","@type": "rdf:Graph","@graph": {"@id": "urn:temp:7","@type": "oa:Annotation","hasBody": "http://paolociccarese.info","hasTarget": {"@id": "urn:temp:8","@type": "oa:SpecificResource","hasSelector": {"@id": "urn:temp:9","@type": "oa:FragmentSelector","conformsTo": "http://www.w3.org/TR/media-frags/","value": "xywh=10,10,5,5"},"hasSource": {"@id": "http://www.example.org/images/logo.jpg","@type": "dctypes:Image"}}}}]}}'	
	
	// Not sure abou this one
	// curl -i -X POST http://localhost:8080/s/annotation --header "Content-Type: application/json" --data '{"apiKey": "testkey","item": {"@context": "https://gist.github.com/hubgit/6105255/raw/36f89110f7cb28fb605f7722048167d82644f946/open-annotation-context.json","@graph": [{"@id": "urn:temp:3","@type": "rdf:Graph","@graph": {"@id": "urn:temp:4","@type": "oa:Annotation","hasBody": {"@id": "urn:temp:5","@type": "rdf:Graph","@graph": {"@id": "http://www.example.org/artifact1","label": "yolo body"}},"hasTarget": "http://www.example.org/target1"}}]}}'
	
	
	def save = {
		long startTime = System.currentTimeMillis();
		
		def apiKey = request.JSON.apiKey;
		boolean allowed = (
			grailsApplication.config.annotopia.storage.testing.enabled=='true' &&
			apiKey==grailsApplication.config.annotopia.storage.testing.apiKey
		);
		if(!allowed) {
			def json = JSON.parse('{"status":"rejected" ,"message":"Api Key missing or invalid"}');
			render(status: 401, text: json, contentType: "text/json", encoding: "UTF-8");
			return;
		}
		
		def item = request.JSON.item
		def flavor = (request.JSON.flavor!=null)?request.JSON.flavor:"OA";
		def validate = (request.JSON.validate!=null)?request.JSON.validate:"OFF";
				
		if(item!=null) {
			log.warn("[" + apiKey + "] TODO: Validation of the Annotation content requested but not implemented yest!");
			
			Dataset dataset = DatasetFactory.createMem();
			
			try {
				// Using the RIOT reader
				RDFDataMgr.read(dataset, new ByteArrayInputStream(item.toString().getBytes("UTF-8")), JenaJSONLD.JSONLD);
				
//				ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//				RDFDataMgr.write(outputStream, dataset, JenaJSONLD.JSONLD);
//				println outputStream.toString();
				
			} catch (Exception ex) {
				HashMap<String,Object> errorResult = new HashMap<String, Object>();
				//errorResult.put("exception", createException("Content parsing failed", "Failure while: loading of the content to validate " + ex.toString()));
				//finalResult.add(errorResult);
				//return finalResult;
				return errorResult;
			}
			
			// Query all graphs
			log.info("[" + apiKey + "] Graphs detection...");
			int totalDetectedGraphs = 0;
			Set<String> graphsUris = new HashSet<String>();
			Query  sparqlGraphs = QueryFactory.create("PREFIX oa: <http://www.w3.org/ns/oa#> SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o . }}");
			QueryExecution qGraphs = QueryExecutionFactory.create (sparqlGraphs, dataset);
			ResultSet rGraphs = qGraphs.execSelect();
			while (rGraphs.hasNext()) {
				QuerySolution querySolution = rGraphs.nextSolution();
				graphsUris.add(querySolution.get("g").toString());
				totalDetectedGraphs++;
			}
			log.info("[" + apiKey + "] Graphs detected " + totalDetectedGraphs);
			
			// Query for graphs containing annotation
			// See: https://www.mail-archive.com/wikidata-l@lists.wikimedia.org/msg00370.html
			log.info("[" + apiKey + "] Annotation graphs detection...");
			HashMap<String, Model> models = new HashMap<String, Model>();
			boolean graphRepresentationDetected = false;
			int totalDetectedAnnotationGraphs = 0;
			Set<String> annotationsGraphsUris = new HashSet<String>();
			Set<String> annotationUris = new HashSet<String>();
			Query  sparqlAnnotationGraphs = QueryFactory.create("PREFIX oa: <http://www.w3.org/ns/oa#> SELECT ?s ?g WHERE { GRAPH ?g { ?s a oa:Annotation . }}");
			QueryExecution qAnnotationGraphs = QueryExecutionFactory.create (sparqlAnnotationGraphs, dataset);
			ResultSet rAnnotationGraphs = qAnnotationGraphs.execSelect();
			while (rAnnotationGraphs.hasNext()) {
				QuerySolution querySolution = rAnnotationGraphs.nextSolution();
				
				Resource graphUri = querySolution.getResource("g");				
				annotationsGraphsUris.add(graphUri.toString());
				graphsUris.remove(graphUri.toString());
				
				Resource annUri = querySolution.getResource("s");
				annotationUris.add(annUri.toString());
				
				// Add saving data
				addCreationDetails(dataset.getNamedModel(graphUri.toString()), annUri);
				
				graphRepresentationDetected = true;
				totalDetectedAnnotationGraphs++;
			}
			log.info("[" + apiKey + "] Annotation graphs detected " + totalDetectedAnnotationGraphs);

			if(annotationsGraphsUris.size()>1) {
				// Annotation Set not found
				log.info("[" + apiKey + "] Multiple Annotation graphs detected... request rejected.");
				def json = JSON.parse('{"status":"nocontent" ,"message":"The request carries multiple Annotations or Annotation Graphs"' +
					',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
				render(status: 200, text: json, contentType: "text/json", encoding: "UTF-8");
				return;
			}
			
			boolean defaultGraphDetected = false;
			if(totalDetectedGraphs==0) {
				Query  sparqlSpecificResources = QueryFactory.create("PREFIX oa: <http://www.w3.org/ns/oa#> SELECT ?s WHERE { ?s a oa:Annotation . }");
				QueryExecution qSpecificResources  = QueryExecutionFactory.create (sparqlSpecificResources, dataset);
				ResultSet rSpecificResources = qSpecificResources.execSelect();
				while (rSpecificResources.hasNext()) {
					QuerySolution querySolution = rSpecificResources.nextSolution();
					Resource annUri = querySolution.getResource("s");
					annotationUris.add(annUri.toString());
					defaultGraphDetected=true;
					
					// Add saving data
					addCreationDetails(dataset.getDefaultModel(), annUri);
				}
			}
			
			if(defaultGraphDetected) {
				log.info("[" + apiKey + "] Default Annotation graphs detected");
			}
						
			// Query Specific Resources
			log.info("[" + apiKey + "] Specific Resources detection...");
			int totalDetectedSpecificResources = 0;
			Set<String> specificResourcesUris = new HashSet<String>();
			Query  sparqlSpecificResources = QueryFactory.create("PREFIX oa: <http://www.w3.org/ns/oa#> SELECT DISTINCT ?s WHERE " +
				"{{ GRAPH ?g { ?s a oa:SpecificResource . }} UNION { ?s a oa:SpecificResource } FILTER (!isBlank(?s))}");
			QueryExecution qSpecificResources  = QueryExecutionFactory.create (sparqlSpecificResources, dataset);
			ResultSet rSpecificResources = qSpecificResources.execSelect();
			while (rSpecificResources.hasNext()) {
				QuerySolution querySolution = rSpecificResources.nextSolution();
				specificResourcesUris.add(querySolution.get("s").toString());
				totalDetectedSpecificResources++;
			}
			log.info("[" + apiKey + "] Identifiable Specific Resources detected " + totalDetectedSpecificResources);
			
			// Query Specific Resources
			log.info("[" + apiKey + "] Content as Text detection...");
			int totalEmbeddedTextualBodies = 0;
			Set<String> embeddedTextualBodiesUris = new HashSet<String>();
			Query  sparqlEmbeddedTextualBodies = QueryFactory.create("PREFIX cnt:<http://www.w3.org/2011/content#> SELECT DISTINCT ?s WHERE " +
				"{{ GRAPH ?g { ?s a cnt:ContentAsText . }} UNION { ?s a cnt:ContentAsText . } FILTER (!isBlank(?s)) }");
			QueryExecution qEmbeddedTextualBodies  = QueryExecutionFactory.create (sparqlEmbeddedTextualBodies, dataset);
			ResultSet rEmbeddedTextualBodies = qEmbeddedTextualBodies.execSelect();
			while (rEmbeddedTextualBodies.hasNext()) {
				QuerySolution querySolution = rEmbeddedTextualBodies.nextSolution();
				embeddedTextualBodiesUris.add(querySolution.get("s").toString());
				println querySolution.get("s").toString()
				println querySolution.get("s").getURI()
				totalEmbeddedTextualBodies++;
			}
			log.info("[" + apiKey + "] Identifiable Content as Text detected " + totalEmbeddedTextualBodies);
			
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			RDFDataMgr.write(outputStream, dataset, JenaJSONLD.JSONLD);
			String content = outputStream.toString();
			
			if(graphRepresentationDetected) {
				annotationsGraphsUris.each {
					def uri = org.annotopia.grails.services.storage.utils.UUID.uuid();
					println '*annotation graph ' + it + ' with uri ' + uri
					content = content.replaceAll(Pattern.quote(it), 
						'http://' + grailsApplication.config.grails.server.host + ':' + 
						grailsApplication.config.grails.server.port.http + '/s/graph/' + uri);
				}
				annotationUris.each {
					def uri = org.annotopia.grails.services.storage.utils.UUID.uuid();
					println '*******annotation ' + it + ' with uri ' + uri
					content = content.replaceAll(Pattern.quote(it), 
						'http://' + grailsApplication.config.grails.server.host + ':' + 
						grailsApplication.config.grails.server.port.http + '/s/annotation/' + uri);
				}
				graphsUris.each {
					def uri = org.annotopia.grails.services.storage.utils.UUID.uuid();
					println '************graph ' + it + ' with uri ' + uri
					content = content.replaceAll(Pattern.quote(it), 
						'http://' + grailsApplication.config.grails.server.host + ':' + 
						grailsApplication.config.grails.server.port.http + '/s/graph/' + uri);
				}
				specificResourcesUris.each {
					def uri = org.annotopia.grails.services.storage.utils.UUID.uuid();
					println '************spres ' + it + ' with uri ' + uri
					content = content.replaceAll(Pattern.quote(it),
						'http://' + grailsApplication.config.grails.server.host + ':' +
						grailsApplication.config.grails.server.port.http + '/s/resource/' + uri);
				}
				embeddedTextualBodiesUris.each {
					def uri = org.annotopia.grails.services.storage.utils.UUID.uuid();
					println '************cntat ' + it + ' with uri ' + uri
					content = content.replaceAll(Pattern.quote(it),
						'http://' + grailsApplication.config.grails.server.host + ':' +
						grailsApplication.config.grails.server.port.http + '/s/content/' + uri);
				}
			} else if(defaultGraphDetected) {
				annotationUris.each {
					def uri = org.annotopia.grails.services.storage.utils.UUID.uuid();
					println '*******annotation ' + it + ' with uri ' + uri
					content = content.replaceAll(Pattern.quote(it),
						'http://' + grailsApplication.config.grails.server.host + ':' +
						grailsApplication.config.grails.server.port.http + '/s/annotation/' + uri);
				}
				specificResourcesUris.each {
					def uri = org.annotopia.grails.services.storage.utils.UUID.uuid();
					println '************spres ' + it + ' with uri ' + uri
					content = content.replaceAll(Pattern.quote(it),
						'http://' + grailsApplication.config.grails.server.host + ':' +
						grailsApplication.config.grails.server.port.http + '/s/resource/' + uri);
				}
				embeddedTextualBodiesUris.each {
					def uri = org.annotopia.grails.services.storage.utils.UUID.uuid();
					println '************cntat ' + it + ' with uri ' + uri
					content = content.replaceAll(Pattern.quote(it),
						'http://' + grailsApplication.config.grails.server.host + ':' +
						grailsApplication.config.grails.server.port.http + '/s/content/' + uri);
				}
			} else {
				// Annotation Set not found
				log.info("[" + apiKey + "] Annotation not found " + content);
				def json = JSON.parse('{"status":"nocontent" ,"message":"The request does not carry acceptable payload or payload cannot be read"' +
					',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
				render(status: 200, text: json, contentType: "text/json", encoding: "UTF-8");
				return;
			}
			
			Dataset dataset2 = DatasetFactory.createMem();
			if(defaultGraphDetected) {
				RDFDataMgr.read(dataset2, new ByteArrayInputStream(content.toString().getBytes("UTF-8")), JenaJSONLD.JSONLD);
							
				// Swap blank nodes with URI
				swapBlankNodesWithURIs(
					dataset2.getDefaultModel(), 
					ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), 
					ResourceFactory.createResource("http://www.w3.org/2011/content#ContentAsText"), "content");
				
				def uri = org.annotopia.grails.services.storage.utils.UUID.uuid();
				Dataset dataset3 = DatasetFactory.createMem();
				dataset3.addNamedModel('http://' + grailsApplication.config.grails.server.host + ':' + 
						grailsApplication.config.grails.server.port.http + '/s/graph/' + uri, dataset2.getDefaultModel());		
				
				//virtuosoJenaStoreService.storeDataset(apiKey, dataset3);
				
				response.contentType = "text/json;charset=UTF-8"
				response.outputStream << '{"status":"saved", "result": {' +
					'"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' +
					'"item":['
						
				RDFDataMgr.write(response.outputStream, dataset3, JenaJSONLD.JSONLD);
				
				response.outputStream <<  ']}}';
				response.outputStream.flush()
				
			} else {
				RDFDataMgr.read(dataset2, new ByteArrayInputStream(content.toString().getBytes("UTF-8")), JenaJSONLD.JSONLD);
				
				response.contentType = "text/json;charset=UTF-8"
				response.outputStream << '{"status":"saved", "result": {' +
					'"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' +
					'"item":['
						
				//response.outputStream << content
						
				RDFDataMgr.write(response.outputStream, dataset2, JenaJSONLD.JSONLD);
				
				response.outputStream <<  ']}}';
				response.outputStream.flush()
			}
			
			
		} else {
			// Annotation Set not found
			log.info("[" + apiKey + "] Annotation not found");
			def json = JSON.parse('{"status":"nocontent" ,"message":"The request does not carry the payload or payload cannot be read"' +
				',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
			render(status: 200, text: json, contentType: "text/json", encoding: "UTF-8");
		}
	}
	
	private void swapBlankNodesWithURIs(Model model, Property property, Resource resource, String uriType) {
		def uuid = org.annotopia.grails.services.storage.utils.UUID.uuid();
		def nUri = 'http://' + grailsApplication.config.grails.server.host + ':' + 
						grailsApplication.config.grails.server.port.http + '/s/' + uriType + '/' + uuid;
		def rUri = ResourceFactory.createResource(nUri);

		def blankNode;
		List<Statement> s = new ArrayList<Statement>();
		StmtIterator statements = model.listStatements(null, property, resource);
		statements.each {
			blankNode =  it.getSubject()
			StmtIterator statements2 = model.listStatements(it.getSubject(), null, null);
			statements2 .each { its ->
				s.add(model.createStatement(rUri, its.getPredicate(), its.getObject()));
			}
		}
		model.removeAll(blankNode, null, null);
	
		StmtIterator statements3 = model.listStatements(null, null, blankNode);
		statements3.each { its2 ->
			s.add(model.createStatement(its2.getSubject(), its2.getPredicate(), rUri));
		}
		model.removeAll(null, null, blankNode);
		
		s.each {
			model.add(it);
		}
	}
	
	private void addCreationDetails(Model model, Resource resource) {
		model.add(resource, ResourceFactory.createProperty("http://purl.org/pav/createdAt"), ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
		model.add(resource, ResourceFactory.createProperty("http://purl.org/pav/lastUpdatedOn"), ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));
	}
	
	static String getCurrentUrl(HttpServletRequest request){
		StringBuilder sb = new StringBuilder()
		sb << request.getRequestURL().substring(0,request.getRequestURL().indexOf("/", 7))
		sb << request.getAttribute("javax.servlet.forward.request_uri")
		if(request.getAttribute("javax.servlet.forward.query_string")){
			sb << "?"
			sb << request.getAttribute("javax.servlet.forward.query_string")
		}
		return sb.toString();
	}
}
