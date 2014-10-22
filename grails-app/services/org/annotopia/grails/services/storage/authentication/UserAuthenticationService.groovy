package org.annotopia.grails.services.storage.authentication

class UserAuthenticationService {

	def grailsApplication;
	def configAccessService;
	
	def getUserId(def ip) {
		log.info("Retrieving User ID on request from IP: " + ip);		
		// Validation mockup for testing mode
		if(configAccessService.getAsString("annotopia.storage.testing.enabled")=='true' &&
				configAccessService.getAsString("annotopia.storage.testing.user.id").length()>0)
			return configAccessService.getAsString("annotopia.storage.testing.user.id");
		else
			return null;
	}
}
