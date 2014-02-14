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

/**
 * This is the REST API for managing Annotation.
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class AnnotationController {
	
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssz")

	def annotationJenaStorageService;
	
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
			
			println item
			
			Dataset dataset = DatasetFactory.createMem();
			
			try {
				// Using the RIOT reader
				RDFDataMgr.read(dataset, new ByteArrayInputStream(item.toString().getBytes("UTF-8")), "http://localhost/jsonld/", JenaJSONLD.JSONLD);
			} catch (Exception ex) {
				HashMap<String,Object> errorResult = new HashMap<String, Object>();
				errorResult.put("exception", createException("Content parsing failed", "Failure while: loading of the content to validate " + ex.toString()));
				//finalResult.add(errorResult);
				//return finalResult;
				return errorResult;
			}
			
			// Query all graphs
			log.info("Graphs detection...");
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
			log.info("Graphs detected " + totalDetectedGraphs);
			
			// Query for graphs containing annotation
			// See: https://www.mail-archive.com/wikidata-l@lists.wikimedia.org/msg00370.html
			log.info("Annotation graphs detection...");
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
				annotationsGraphsUris.add(querySolution.get("g").toString());
				graphsUris.remove(querySolution.get("g").toString());
				annotationUris.add(querySolution.get("s").toString());
				graphRepresentationDetected = true;
				totalDetectedAnnotationGraphs++;
			}
			log.info("Annotation graphs detected " + totalDetectedAnnotationGraphs);
			
			if(annotationsGraphsUris.size()>1) {
				// Annotation Set not found
				log.info("[" + apiKey + "] Multiple Annotation graphs detected");
				def json = JSON.parse('{"status":"nocontent" ,"message":"The request carries multiple Annotations or Annotation Graphs"' +
					',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
				render(status: 200, text: json, contentType: "text/json", encoding: "UTF-8");
				return;
			}
			
			String content = item.toString();
			
			if(graphRepresentationDetected) {
				annotationsGraphsUris.each {
					println '*******annograph ' + it
					content = content.replaceAll(Pattern.quote(it), 
						'http://' + grailsApplication.config.grails.server.host + ':' + grailsApplication.config.grails.server.port.http + '/s/graph/' + 
							org.annotopia.grails.services.storage.utils.UUID.uuid());
				}
				annotationUris.each {
					println '***********annot ' + it
					content = content.replaceAll(Pattern.quote(it), 
						'http://' + grailsApplication.config.grails.server.host + ':' + grailsApplication.config.grails.server.port.http + '/s/annotation/' +
							org.annotopia.grails.services.storage.utils.UUID.uuid());
				}
				graphsUris.each {
					println '***********other ' + it
					content = content.replaceAll(Pattern.quote(it), 
						'http://' + grailsApplication.config.grails.server.host + ':' + grailsApplication.config.grails.server.port.http + '/s/graph/' +
							org.annotopia.grails.services.storage.utils.UUID.uuid());
				}
			}
			
			println content;
			
			
			/*
			if(graphRepresentationDetected) {
				annotationsGraphsUris.each {
					println '*********** ' + it
					
					// TODO validation
					
					// New URIs
					def finalGraphUri = org.annotopia.grails.services.storage.utils.UUID.uuid();
					def finalAnnotationUri = org.annotopia.grails.services.storage.utils.UUID.uuid();
					def graphUri = 'http://' + grailsApplication.config.grails.server.host + ':' + grailsApplication.config.grails.server.port.http + '/s/graph/' + finalGraphUri;
					def annotationUri = 'http://' + grailsApplication.config.grails.server.host + ':' + grailsApplication.config.grails.server.port.http + '/s/annotation/' + finalAnnotationUri;
					
					Model m = dataset.getNamedModel(it);
					
					//models.put(graphUri, m);
				}
			} else {
				println '--- DEFAULT GRAPH CASE'
				models.put("default", dataset.getDefaultModel());
			}
			
			models.keySet().each { key ->
				println key + ' ' + models.get(key);
			}
			
			
			
			// Updating URIs
			println 'graphURI: ' + item["@id"]
			println 'annURI: ' + item['@graph'][0]["@id"]
			
			def graphUriBuffer = item["@id"];
			def annotationUriBuffer = item['@graph'][0]["@id"];
			
			def finalGraphUri = org.annotopia.grails.services.storage.utils.UUID.uuid();
			def finalAnnotationUri = org.annotopia.grails.services.storage.utils.UUID.uuid();
			
			item.put("@id", 'http://' + grailsApplication.config.grails.server.host + ':' + grailsApplication.config.grails.server.port.http + '/s/annotationgraph/' + finalGraphUri);
			item['@graph'][0].put("@id", 'http://' + grailsApplication.config.grails.server.host + ':' + grailsApplication.config.grails.server.port.http + '/s/annotation/' + finalAnnotationUri);
			
			// Updating provenance
			item['@graph'][0].put("http://purl.org/pav/createdOn", dateFormat.format((new Date())));
			item['@graph'][0].put("http://purl.org/pav/lastSavedOn", dateFormat.format((new Date())));			
			
			annotationJenaStorageService.storeAnnotationSet(apiKey, item.toString(), flavor, validate);
			*/
			
			response.contentType = "text/json;charset=UTF-8"
			response.outputStream << '{"status":"saved", "result": {' +
				'"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' +
				'"items":['
					
			// RDFDataMgr.write(response.outputStream, graphs, JenaJSONLD.JSONLD);
			
			response.outputStream <<  ']}}';
			response.outputStream.flush()
		} else {
			// Annotation Set not found
			log.info("[" + apiKey + "] Annotation not found");
			def json = JSON.parse('{"status":"nocontent" ,"message":"The request does not carry the payload or payload cannot be read"' +
				',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
			render(status: 200, text: json, contentType: "text/json", encoding: "UTF-8");
		}
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
