import virtuoso.jena.driver.VirtGraph

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

class BootStrap {

	def grailsApplication
	
	def init = { servletContext ->
		log.info  '========================================================================';
		log.info  ' ANNOTOPIA SMART STORAGE (v.' +
			grailsApplication.metadata['app.version'] + ", b." +
			grailsApplication.metadata['app.build'] + ")";
			
		separator();
		log.info  ' By Paolo Ciccarese (http://paolociccarese.info/)'
		log.info  ' For MIND Informatics (http://mindinformatics.org/)'
		log.info  ' Copyright 2014 Massachusetts General Hospital'
		
		separator();
		log.info  ' Released under the Apache License, Version 2.0'
	    log.info  ' url:http://www.apache.org/licenses/LICENSE-2.0'

		log.info  '========================================================================';
		log.info  'Bootstrapping....'
		
		separator();
		log.info  '** Virtuoso Configuration';
		log.info  ' url        : ' + grailsApplication.config.annotopia.storage.triplestore.host ;
		log.info  ' user       : ' + grailsApplication.config.annotopia.storage.triplestore.user ;
		log.info  ' password   : ' + grailsApplication.config.annotopia.storage.triplestore.pass ;
		log.info  '** Virtuoso Checking Connection';
		try {
			VirtGraph virtGraph = new VirtGraph (
				grailsApplication.config.annotopia.storage.triplestore.host,
				grailsApplication.config.annotopia.storage.triplestore.user,
				grailsApplication.config.annotopia.storage.triplestore.pass);
			log.info  ' ...connection active!'
		} catch (Exception e) {
			log.error e.getMessage();
			terminate();
		}	
		
		separator();
		log.info  '** Services Interceptors';
		for (sc in grailsApplication.serviceClasses) {
			def mc = sc.clazz.metaClass
			mc.invokeMethod = { String name, args ->
				long startTime = System.currentTimeMillis();
				try {
					return mc.getMetaMethod(name, args).invoke(delegate, args)
				}
				finally {
					log.trace "Service method $name with args $args " +
							"took ${System.currentTimeMillis() - startTime}ms"
				}
			}
		}
		
		separator();
		log.info  'Bootstrapping complete!'
		log.info  '========================================================================';
	}
	
	private separator() {
		log.info  '------------------------------------------------------------------------';
	}
	
	private terminate() {
		log.error 'BOOTSTRAPPING TERMINATED, APPLICATION SHUTDOWN!'
		log.error '========================================================================';
		throw new RuntimeException();
	}
}
