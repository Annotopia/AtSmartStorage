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
}
