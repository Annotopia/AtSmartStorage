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
package org.annotopia.grails.controllers.annotation.openannotation;

import static org.junit.Assert.*
import grails.converters.JSON
import grails.test.mixin.TestFor

import org.annotopia.grails.vocabularies.DC
import org.codehaus.groovy.runtime.StackTraceUtils

/**
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
@TestFor(OpenAnnotationController)
class OpenAnnotationControllerTests extends GroovyTestCase {

	def grailsApplication = new org.codehaus.groovy.grails.commons.DefaultGrailsApplication()
	
	private final StringBuilder builder = new StringBuilder();
	private LOG_SEPARATOR() {
		logSeparator('=' as char);
	}
	private logSeparator(char c) {
		if (c==null) c = '=';
		log.info(stringOf(c as char, (68) as int))
	}
	private LOG_TEST_TITLE(String title) {
		int difference = 68-title.length();
		if(difference%2 == 0) { // Even
			log.info('even');
			String pp = stringOf('-' as char, (difference/2) as int)
			log.info(pp + title.toUpperCase() + '-' + pp);
		} else {
			String pp = stringOf('-' as char, (difference/2+1) as int)
			log.info(pp + title.toUpperCase() +  pp);
		}
		logSeparator('-' as char);
	}
	private String stringOf( char c , int times ) {
		for( int i = 0 ; i < times ; i++  ) {
			builder.append( c );
		}
		String result = builder.toString();
		builder.delete( 0 , builder.length() -1 );
		return result;
	}
	private getCurrentMethodName(){
		def marker = new Throwable()
		return StackTraceUtils.sanitize(marker).stackTrace[1].methodName
	}
	  
	// ----------------------------------------
	//  APIKEY Tests
	// ----------------------------------------
	
	void testShowMissingApiKey() {
		LOG_TEST_TITLE(getCurrentMethodName());
		/*
			Testing with command line:
			curl -i -X GET http://localhost:8090/s/annotation \
			-H "Content-Type: application/json" 
		 */

		def c = new OpenAnnotationController()
		c.show()
		
		assertEquals 401, response.status
		LOG_SEPARATOR();
	}
	
	void testShowInvalidApiKey() {
		LOG_TEST_TITLE(getCurrentMethodName());
		/*
		   	Testing with command line:
			curl -i -X GET http://localhost:8090/s/annotation \
			-H "Content-Type: application/json" -d '{"apiKey":"invalidtestkey"}'
		 */

		def c = new OpenAnnotationController()
		c.request.JSON = '{"apiKey":"invalid' + grailsApplication.config.annotopia.storage.testing.apiKey + '"}'
		c.show()
		
		assertEquals 401, response.status
		LOG_SEPARATOR();
	}
	
	void testShowValidApiKey() {
		LOG_TEST_TITLE(getCurrentMethodName());
		/*
		 	Testing with command line:
			curl -i -X GET http://localhost:8090/s/annotation \
			-H "Content-Type: application/json" -d '{"apiKey":"testkey"}'
		 */
		
		def c = new OpenAnnotationController()
		c.request.JSON = '{"apiKey":"' + grailsApplication.config.annotopia.storage.testing.apiKey + '"}'
		c.show()
		
		assertEquals 200, response.status
		LOG_SEPARATOR();
	}
	
	void testValidationInvalidApiKey() {		
		LOG_TEST_TITLE(getCurrentMethodName());
		/*
		 	Testing with command line:
			curl -i -X POST http://localhost:8080/oa/validate \
			-H "Content-Type: application/json" -d '{"apiKey":"invalidtestkey"}'
		 */
		
		def c = new OpenAnnotationController()
		c.validate()
		
		assertEquals 401, response.status
		LOG_SEPARATOR();
	}
	
	void testValidationValidApiKey() {
		LOG_TEST_TITLE(getCurrentMethodName());
		/*
		 	Testing with command line:
		 	curl -i -X POST http://localhost:8090/oa/validate \
		 	-H "Content-Type: application/json" -d '{"apiKey":"testkey"}'	
		 */
		
		def c = new OpenAnnotationController()
		c.request.JSON = '{"apiKey":"' + grailsApplication.config.annotopia.storage.testing.apiKey + '"}'
		c.validate()
		
		assertEquals 200, response.status
		LOG_SEPARATOR();
	}
	
	final NO_CONTENT = "nocontent"
	final TEST_TARGET_URL = "http://www.example.com/pmc/articles/PMC3102893/"
	
	/**
	 * Content without declared oa:Annotation: triggers a 'nocontent' message.
	 */
	void testSimpleAnnotationCreation000() {
		LOG_TEST_TITLE(getCurrentMethodName());
		/*
			Testing with command line:
			curl -i -X POST http://localhost:8090/s/annotation -H "Content-Type: application/json" \
			-d'{"apiKey":"testkey", "item":{"@context":"https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/OAContext.json","@id":"urn:temp:001","hasBody":{"@type":["cnt:ContentAsText","dctypes:Text"],"cnt:chars":"This paper is about Annotation Ontology (AO)","dc:format":"text/plain"},"hasTarget":{"@id":"http://www.ncbi.nlm.nih.gov/pmc/articles/PMC3102894/","@type":"dctypes:Text"}}}'
		*/
		
		String content =
			'{"@context":"https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/OAContext.json",' +
				'"@id":"urn:temp:001",' +
				'"hasBody":{' +
					'"@type":["cnt:ContentAsText","dctypes:Text"],' +
					'"cnt:chars":"This paper is about Annotation Ontology (AO)",' +
					'"dc:format":"text/plain"' +
				'},' +
				'"hasTarget":{' +
					'"@id":"' + TEST_TARGET_URL + '",' +
					'"@type":"dctypes:Text"}' +
				'}' +
			'}';

		def c = new OpenAnnotationController()
		c.request.JSON =
			'{"apiKey":"' + grailsApplication.config.annotopia.storage.testing.apiKey + '",' +
			'"item":' + content + '}';
		c.save()
		
		assertEquals 200, response.status
		
		def resp = JSON.parse(response.text);
		assertEquals NO_CONTENT, resp.statusCode
		log.error(resp.statusMessage);
		LOG_SEPARATOR();
	}
	
	/**
	 * Content without a target: triggers a 'nocontent' message.
	 */
	void testSimpleAnnotationCreation001() {
		LOG_TEST_TITLE(getCurrentMethodName());
		/*
			Testing with command line:
			curl -i -X POST http://localhost:8090/s/annotation -H "Content-Type: application/json" \
			-d'{"apiKey":"testkey", "item":{"@context":"https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/OAContext.json","@id":"urn:temp:001","@type" : "http://www.w3.org/ns/oa#Annotation","hasBody":{"@type":["cnt:ContentAsText","dctypes:Text"],"cnt:chars":"This paper is about Annotation Ontology (AO)","dc:format":"text/plain"}}}'
		*/
		
		String content =
			'{"@context":"https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/OAContext.json",' +
				'"@id":"urn:temp:001",' +
				'"@type" : "http://www.w3.org/ns/oa#Annotation",' +
				'"hasBody":{' +
					'"@type":["cnt:ContentAsText","dctypes:Text"],' +
					'"cnt:chars":"This paper is about Annotation Ontology (AO)",' +
					'"dc:format":"text/plain"' +
				'}' +
			'}';

		def c = new OpenAnnotationController()
		c.request.JSON =
			'{"apiKey":"' + grailsApplication.config.annotopia.storage.testing.apiKey + '",' +
			'"item":' + content + '}';
		c.save()
		
		assertEquals 200, response.status
		
		def resp = JSON.parse(response.text);
		assertEquals NO_CONTENT, resp.statusCode
		log.error(resp.statusMessage);
		
		LOG_SEPARATOR();
	}
	
	/**
	 * Content with target and no body.
	 */
	void testSimpleAnnotationCreation002() {
		LOG_TEST_TITLE(getCurrentMethodName());
		/*
			Testing with command line:
			curl -i -X POST http://localhost:8090/s/annotation -H "Content-Type: application/json" \
			-d'{"apiKey":"testkey", "outCmd":"frame", "item":{"@context":"https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/OAContext.json","@id":"urn:temp:001","@type":"http://www.w3.org/ns/oa#Annotation","hasTarget":{"@id":"http://www.ncbi.nlm.nih.gov/pmc/articles/PMC3102894/","@type":"dctypes:Text"}}}'
		*/
		
		String content =
			'{"@context":"https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/OAContext.json",' +
				'"@id":"urn:temp:001",' +
				'"@type":"http://www.w3.org/ns/oa#Annotation",' +
				'"hasTarget":{' +
					'"@id":"' + TEST_TARGET_URL + '",' +
					'"@type":"dctypes:Text"' +
				'}' +
			'}';

		def c = new OpenAnnotationController()
		c.request.JSON =
			'{"apiKey":"' + grailsApplication.config.annotopia.storage.testing.apiKey + '",' +
			'"item":' + content + ',"outCmd":"frame"}';
		c.save()
		
		assertEquals 200, response.status
		
		def resp = JSON.parse(response.text);
		def annName = resp['result']['item'][0]['@graph'][0]['@id']
		
		log.info("Removing annotation " + annName);
		c.request.JSON << JSON.parse('{"apiKey":"' + grailsApplication.config.annotopia.storage.testing.apiKey + '","uri":"' + annName + '"}')
		c.delete();
		
		LOG_SEPARATOR();
	}
	
	/**
	 * Content with target and body.
	 */
	void testSimpleAnnotationCreation003() {
		LOG_TEST_TITLE(getCurrentMethodName());
		/*
			Testing with command line:
			curl -i -X POST http://localhost:8090/s/annotation -H "Content-Type: application/json" \
			-d'{"apiKey":"testkey", "outCmd":"frame", "item":{"@context":"https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/OAContext.json","@id":"urn:temp:001","@type":"http://www.w3.org/ns/oa#Annotation","hasTarget":{"@id":"http://www.ncbi.nlm.nih.gov/pmc/articles/PMC3102893/","@type":"dctypes:Text"},"hasBody":{"@type":["cnt:ContentAsText","dctypes:Text"],"cnt:chars":"This paper is about Annotation Ontology (AO)","dc:format":"text/plain"}}}'
		*/
		
		String content =
			'{"@context":"https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/OAContext.json",' +
				'"@id":"urn:temp:001",' +
				'"@type":"http://www.w3.org/ns/oa#Annotation",' +
				'"hasTarget":{' +
					'"@id":"' + TEST_TARGET_URL + '",' +
					'"@type":"dctypes:Text"' +
				'},' +
				'"hasBody":{' +
					'"@type":["cnt:ContentAsText","dctypes:Text"],' +
					'"cnt:chars":"This paper is about Annotation Ontology (AO)",' +
					'"dc:format":"text/plain"' +
				'}' +
			'}';

		def c = new OpenAnnotationController()
		c.request.JSON =
			'{"apiKey":"' + grailsApplication.config.annotopia.storage.testing.apiKey + '",' +
			'"item":' + content + ',"outCmd":"frame"}';
		c.save()
		
		assertEquals 200, response.status
		
		def resp = JSON.parse(response.text);
		def annUri = resp['result']['item'][0]['@graph'][0]['@id']
		
		log.info("Removing annotation " + annUri);
		c.request.JSON << JSON.parse('{"apiKey":"' + grailsApplication.config.annotopia.storage.testing.apiKey + '","uri":"' + annUri + '"}')
		c.delete();
		
		assertEquals 200, response.status
		
		LOG_SEPARATOR();
	}
	
	/**
	 * Content with full target and a textual body.
	 */
	void testSimpleAnnotationCreationAndRetrieval001() {
		LOG_TEST_TITLE(getCurrentMethodName());
		/*
			Testing with command line:
			curl -i -X POST http://localhost:8090/s/annotation -H "Content-Type: application/json" \
			-d'{"apiKey":"testkey", "outCmd":"frame", "item":{"@context":"https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/OAContext.json","@id":"urn:temp:001","@type":"http://www.w3.org/ns/oa#Annotation","hasTarget":{"@id":"http://www.ncbi.nlm.nih.gov/pmc/articles/PMC3102893/","@type":"dctypes:Text"},"hasBody":{"@type":["cnt:ContentAsText","dctypes:Text"],"cnt:chars":"This paper is about Annotation Ontology (AO)","dc:format":"text/plain"}}}'
		*/
		
		String content =
			'{"@context":"https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/OAContext.json",' +
				'"@id":"urn:temp:001",' +
				'"@type":"http://www.w3.org/ns/oa#Annotation",' +
				'"hasTarget":{' +
					'"@id":"' + TEST_TARGET_URL + '",' +
					'"@type":"dctypes:Text"' +
				'},' +
				'"hasBody":{' +
					'"@type":["cnt:ContentAsText","dctypes:Text"],' +
					'"cnt:chars":"This paper is about Annotation Ontology (AO)",' +
					'"dc:format":"text/plain"' +
				'}' +
			'}';

		def c = new OpenAnnotationController()
		c.request.JSON =
			'{"apiKey":"' + grailsApplication.config.annotopia.storage.testing.apiKey + '",' +
			'"item":' + content + ',"outCmd":"frame"}';
		c.save()
		
		assertEquals 200, response.status
		
		log.info("Saved: " + response.text);
		
		def resp = JSON.parse(response.text);
		def annUri = resp['result']['item'][0]['@graph'][0]['@id']
		
		c.response.reset();
		c.request.JSON.clear();
		
		log.info("Retrieving annotation by target URL " + TEST_TARGET_URL);
		c.request.JSON =
			'{"apiKey":"' + grailsApplication.config.annotopia.storage.testing.apiKey + '",' +
			'"tgtUrl":"' + TEST_TARGET_URL + '","outCmd":"frame"}';
		c.show();
		
		assertEquals 200, response.status
		
		log.trace("Retrieved: " + response.text);
		resp = JSON.parse(response.text);
		
		log.info("Verifying annotation id");
		def respAnnUri = resp['result']['items'][0]['@graph'][0]['@id']
		assertEquals annUri, respAnnUri
		
		log.info("Verifying annotation motivation");
		def respAnnMotivation = resp['result']['items'][0]['@graph'][0]['hasMotivation']
		assertNull respAnnMotivation
		
		log.info("Verifying annotation target");
		def respAnnTarget = resp['result']['items'][0]['@graph'][0]['hasTarget']
		assertEquals respAnnTarget["@id"], TEST_TARGET_URL
		assertEquals respAnnTarget["@type"], "dctypes:Text"
		
		log.info("Verifying annotation body");
		def respAnnBody = resp['result']['items'][0]['@graph'][0]['hasBody']
		assertEquals respAnnBody["format"], "text/plain"
		assertNotNull respAnnBody["chars"]
		assertEquals respAnnBody["@type"].size(), 2
		
		log.info("Removing annotation " + annUri);
		c.request.JSON << JSON.parse('{"apiKey":"' + grailsApplication.config.annotopia.storage.testing.apiKey + '","uri":"' + annUri + '"}')
		c.delete();
		
		assertEquals 200, response.status
		
		LOG_SEPARATOR();
	}
	
	/**
	 * Content with full target and a textual comment body.
	 */
	void testSimpleAnnotationCreationAndRetrieval002() {
		LOG_TEST_TITLE(getCurrentMethodName());
		/*
			 Testing with command line:
			 curl -i -X POST http://localhost:8090/s/annotation -H "Content-Type: application/json" \
			 -d'{"apiKey":"testkey", "outCmd":"frame", "item":{"@context":"https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/OAContext.json","@id":"urn:temp:001","@type":"http://www.w3.org/ns/oa#Annotation","motivatedBy":"oa:commenting","hasTarget":{"@id":"http://www.ncbi.nlm.nih.gov/pmc/articles/PMC3102893/","@type":"dctypes:Text"},"hasBody":{"@type":["cnt:ContentAsText","dctypes:Text"],"cnt:chars":"This paper is about Annotation Ontology (AO)","dc:format":"text/plain"}}}'
		 */
		
		String content =
		'{"@context":"https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/OAContext.json",' +
			'"@id":"urn:temp:001",' +
			'"@type":"http://www.w3.org/ns/oa#Annotation",' +
			'"motivatedBy": "oa:commenting",' +
			'"hasTarget":{' +
				'"@id":"' + TEST_TARGET_URL + '",' +
				'"@type":"dctypes:Text"' +
			'},' +
			'"hasBody":{' +
				'"@type":["cnt:ContentAsText","dctypes:Text"],' +
				'"cnt:chars":"This paper is about Annotation Ontology (AO)",' +
				'"dc:format":"text/plain"' +
			'}' +
		'}';

		def c = new OpenAnnotationController()
		c.request.JSON =
			'{"apiKey":"' + grailsApplication.config.annotopia.storage.testing.apiKey + '",' +
			'"item":' + content + ',"outCmd":"frame"}';
		c.save()
		
		assertEquals 200, response.status
		
		log.info("Saved: " + response.text);
		
		def resp = JSON.parse(response.text);
		def annUri = resp['result']['item'][0]['@graph'][0]['@id']
		
		c.response.reset();
		c.request.JSON.clear();
		
		log.info("Retrieving annotation by target URL " + TEST_TARGET_URL);
		c.request.JSON =
			'{"apiKey":"' + grailsApplication.config.annotopia.storage.testing.apiKey + '",' +
			'"tgtUrl":"' + TEST_TARGET_URL + '","outCmd":"frame"}';
		c.show();
		
		assertEquals 200, response.status
		
		log.trace("Retrieved: " + response.text);
		resp = JSON.parse(response.text);
		
		log.info("Verifying annotation id");
		def respAnnUri = resp['result']['items'][0]['@graph'][0]['@id']
		assertEquals annUri, respAnnUri
		
		log.info("Verifying annotation motivation");
		def respAnnMotivation = resp['result']['items'][0]['@graph'][0]['motivatedBy']
		assertEquals 'oa:commenting', respAnnMotivation
		
		log.info("Verifying annotation target");
		def respAnnTarget = resp['result']['items'][0]['@graph'][0]['hasTarget']
		assertEquals respAnnTarget["@id"], TEST_TARGET_URL
		assertEquals respAnnTarget["@type"], "dctypes:Text"
		
		log.info("Verifying annotation body");
		def respAnnBody = resp['result']['items'][0]['@graph'][0]['hasBody']
		assertEquals respAnnBody["format"], "text/plain"
		assertNotNull respAnnBody["chars"]
		assertEquals respAnnBody["@type"].size(), 2
		
		log.info("Removing annotation " + annUri);
		c.request.JSON << JSON.parse('{"apiKey":"' + grailsApplication.config.annotopia.storage.testing.apiKey + '","uri":"' + annUri + '"}')
		c.delete();
		
		assertEquals 200, response.status
		
		LOG_SEPARATOR();
	}
	
	/**
	 * Content with full target and a textual comment body.
	 */
	void testSimpleAnnotationCreationAndRetrieval003() {
		LOG_TEST_TITLE(getCurrentMethodName());
		/*
			 Testing with command line:
			 curl -i -X POST http://localhost:8090/s/annotation -H "Content-Type: application/json" \
			 -d'{"apiKey":"testkey", "outCmd":"frame", "item":{"@context":"https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/OAContext.json","@id":"urn:temp:001","@type":"http://www.w3.org/ns/oa#Annotation","annotatedBy":{"@id":"http://orcid.org/0000-0002-5156-2703","@type":"foaf:Person","foaf:name":"Paolo Ciccarese"},"hasMotivation":"oa:commenting","hasTarget":{"@id":"http://www.ncbi.nlm.nih.gov/pmc/articles/PMC3102893/","@type":"dctypes:Text"},"hasBody":{"@type":["cnt:ContentAsText","dctypes:Text"],"cnt:chars":"This paper is about Annotation Ontology (AO)","dc:format":"text/plain"}}}'
		 */
		
		String content =
		'{"@context":"https://raw2.github.com/Annotopia/AtSmartStorage/master/web-app/data/OAContext.json",' +
			'"@id":"urn:temp:001",' +
			'"@type":"http://www.w3.org/ns/oa#Annotation",' +
			'"motivatedBy": "oa:commenting",' +
			'"annotatedBy": {' +
				'"@id": "http://orcid.org/0000-0002-5156-2703",' +
				'"@type": "foaf:Person",' +
				'"foaf:name": "Paolo Ciccarese"' +
			'},' +
			'"hasTarget":{' +
				'"@id":"' + TEST_TARGET_URL + '",' +
				'"@type":"dctypes:Text"' +
			'},' +
			'"hasBody":{' +
				'"@type":["cnt:ContentAsText","dctypes:Text"],' +
				'"cnt:chars":"This paper is about Annotation Ontology (AO)",' +
				'"dc:format":"text/plain"' +
			'}' +
		'}';
		
		def c = new OpenAnnotationController()
		c.request.JSON =
			'{"apiKey":"' + grailsApplication.config.annotopia.storage.testing.apiKey + '",' +
			'"item":' + content + ',"outCmd":"frame"}';
		c.save()
		
		assertEquals 200, response.status
		
		log.info("Saved: " + response.text);
		
		def resp = JSON.parse(response.text);
		def annUri = resp['result']['item'][0]['@graph'][0]['@id']
		
		c.response.reset();
		c.request.JSON.clear();
		
		log.info("Retrieving annotation by target URL " + TEST_TARGET_URL);
		c.request.JSON =
			'{"apiKey":"' + grailsApplication.config.annotopia.storage.testing.apiKey + '",' +
			'"tgtUrl":"' + TEST_TARGET_URL + '","outCmd":"frame"}';
		c.show();
		
		assertEquals 200, response.status
		
		log.trace("Retrieved: " + response.text);
		resp = JSON.parse(response.text);
		
		log.info("Verifying annotation id");
		def respAnnUri = resp['result']['items'][0]['@graph'][0]['@id']
		assertEquals annUri, respAnnUri
		
		log.info("Verifying annotator info");
		def respAnnotator = resp['result']['items'][0]['@graph'][0]['annotatedBy']
		assertEquals "http://orcid.org/0000-0002-5156-2703", respAnnotator["@id"]
		assertEquals "foaf:Person", respAnnotator["@type"]
		assertEquals "Paolo Ciccarese", respAnnotator["name"]
		
		log.info("Removing annotation " + annUri);
		c.request.JSON << JSON.parse('{"apiKey":"' + grailsApplication.config.annotopia.storage.testing.apiKey + '","uri":"' + annUri + '"}')
		c.delete();
		
		assertEquals 200, response.status
		
		LOG_SEPARATOR();
	}
}
