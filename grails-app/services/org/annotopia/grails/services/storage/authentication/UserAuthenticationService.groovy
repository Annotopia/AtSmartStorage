package org.annotopia.grails.services.storage.authentication

class UserAuthenticationService {

	def grailsApplication;
	
	def getUserId(def ip) {
		log.info("Retrieving User ID on request from IP: " + ip);		
		// Validation mockup for testing mode
		if(grailsApplication.config.annotopia.storage.testing.enabled=='true' &&
				grailsApplication.config.annotopia.storage.testing.user.id!=null)
			return grailsApplication.config.annotopia.storage.testing.user.id;
		else
			return null;
	}
}
