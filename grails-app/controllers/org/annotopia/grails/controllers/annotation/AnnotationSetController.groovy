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

import javax.servlet.http.HttpServletRequest

import org.apache.jena.riot.RDFDataMgr

import com.github.jsonldjava.jena.JenaJSONLD
import com.hp.hpl.jena.query.Dataset

/**
 * This is the REST API for managing Annotation Sets.
 *
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class AnnotationSetController {

	def virtuosoJenaStoreService
	def annotationJenaStorageService
	
	// curl -X PUT -d arg=val -d arg2=val2 http://localhost:8080/AtSmartStorage/annotationSet/
	
	// curl -i -X GET http://localhost:8080/AtSmartStorage/annotationset
	// curl -i -X GET http://localhost:8080/AtSmartStorage/annotationset --header "Content-Type: application/json" --data '{"apiKey":"testkey"}'
	// curl -i -X GET http://localhost:8080/AtSmartStorage/annotationset --header "Content-Type: application/json" --data '{"apiKey":"testkey" ,  "tgtUrl":"http://www.jbiomedsem.com/content/2/S2/S4"}'
	// curl -i -X GET http://localhost:8080/AtSmartStorage/annotationset --header "Content-Type: application/json" --data '{"apiKey":"testkey" ,  "tgtUrl":"http://www.jbiomedsem.com/content/2/S2/S4", "tgtFgt":"true"}'
	// curl -i -X GET http://localhost:8080/AtSmartStorage/annotationset/86 --header "Content-Type: application/json" --data '{"apiKey":"testkey"}'
	// curl -i -X GET http://localhost:8080/AtSmartStorage/annotationset --header "Content-Type: application/json" --data '{"apiKey":"testkey" ,  "tgtUrl":"http://www.jbiomedsem.com/content/2/S2/S4", "tgtFgt":"true", "max":"1"}'
	
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
			def max = (request.JSON.max!=null)?request.JSON.max:10;
			def offset = (request.JSON.offset!=null)?request.JSON.offset:0;
			
			// Target filters
			def tgtUrl = request.JSON.tgtUrl
			def tgtFgt = request.JSON.tgtFgt
			def tgtExt = request.JSON.tgtExt
			def tgtIds = request.JSON.tgtIds
			log.info("[" + apiKey + "] List >>" + 
				" max:" + max +
				" offset:" + offset +
				((tgtUrl!=null) ? (" tgtUrl:" + tgtUrl):"") +
				((tgtFgt!=null) ? (" tgtFgt:" + tgtFgt):"") +
				((tgtExt!=null) ? (" tgtExt:" + tgtExt):"") + 
				((tgtIds!=null) ? (" tgtIds:" + tgtIds):""));
			
			def result = ']}'
	
			int total = annotationJenaStorageService.countAnnotationGraphs(apiKey, tgtUrl, tgtFgt);
			int pages = (total/Integer.parseInt(max));
			
			if(total>0 && Integer.parseInt(offset)>=pages) {
				log.info("[" + apiKey + "] The requested page " + offset + " does not exist, the page index limit is " + (pages==0?"0":(pages-1)) );
				def json = JSON.parse('{"status":"rejected" ,"message":"The requested page ' + offset + ' does not exist, the page index limit is ' + (pages==0?"0":(pages-1))+ '"' + 
					',"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' + '}');
				render(status: 401, text: json, contentType: "text/json", encoding: "UTF-8");
				return;
			}
			
			Dataset graphs = annotationJenaStorageService.listAnnotationSet(apiKey, max, offset, tgtUrl, tgtFgt, tgtExt, tgtIds);
	
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
				log.info("[" + apiKey + "] No Annotation Sets found with the specified criteria");
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
			Dataset graphs =  annotationJenaStorageService.retrieveAnnotationGraph(apiKey, getCurrentUrl(request));
			
			if(!graphs.getNamedModel(getCurrentUrl(request)).isEmpty()) {
				response.contentType = "text/json;charset=UTF-8"
				response.outputStream << '{"status":"result", "result": {' +
					'"duration": "' + (System.currentTimeMillis()-startTime) + 'ms", ' +
					'"items":['
						
				RDFDataMgr.write(response.outputStream, graphs, JenaJSONLD.JSONLD);
				
				response.outputStream <<  ']}}';
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
	
	// curl -i -X POST http://localhost:8080/AtSmartStorage/annotationset
	// curl -i -X POST http://localhost:8080/AtSmartStorage/annotationset --header "Content-Type: application/json" --data '{"apiKey":"testkey"}'
	// curl -i -X POST http://localhost:8080/AtSmartStorage/annotationset --header "Content-Type: application/json" --data '{"apiKey":"testkey", "set":{"@context":"https://gist.github.com/hubgit/6105255/raw/36f89110f7cb28fb605f7722048167d82644f946/open-annotation-context.json" ,"@graph" : [ {"@id" : "http://www.example.org/ann1","@type" : "http://www.w3.org/ns/oa#Annotation","http://www.w3.org/ns/oa#hasBody" : {"@id" : "http://www.example.org/body3"},"http://www.w3.org/ns/oa#hasTarget" : {"@id" : "http://www.example.org/target3"} } ],"@id" : "http://annotopiaserver.org/annotationset/90"}}'
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
		
		if(request.JSON.set!=null) {
			log.warn("TODO: Validation of the annotation content!");
			virtuosoJenaStoreService.store(apiKey, request.JSON.set.toString(), "");
			render 'saving set \n';
			return;
		} else {
			// Annotation Set not found
			log.info("[" + apiKey + "] Annotation set not found");
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
