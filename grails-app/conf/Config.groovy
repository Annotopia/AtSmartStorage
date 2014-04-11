import grails.util.Metadata

import org.apache.log4j.RollingFileAppender

grails.app.context="/"

// Necessary for Grails 2.0 as the variable ${appName} is not available 
// anymore in the log4j closure. It needs the import above.
def appName = Metadata.current.getApplicationName();

// locations to search for config files that get merged into the main config
// config files can either be Java properties files or ConfigSlurper scripts
// See: http://stackoverflow.com/questions/3807267/grails-external-configuration-grails-config-locations-absolute-path-file
grails.config.locations = ["classpath:${appName}-config.properties", "file:./${appName}-config.properties",
						   "classpath:${appName}-debug.properties", "file:./${appName}-debug.properties"]

grails.resources.adhoc.patterns = ['/data/*', "*.json"]

// layout:pattern(conversionPattern: '%d{dd MMM yyyy HH:mm:ss,SSS} %5p %c{2} %m%n')

environments {
	development {
		log4j = {
		    appenders {
			    console name:'stdout', threshold: org.apache.log4j.Level.TRACE, 
					layout:pattern(conversionPattern: '%d{mm:ss,SSS} %5p %c{1} %m%n')
			}
		
		    error  'org.codehaus.groovy.grails.web.servlet',  //  controllers
		           'org.codehaus.groovy.grails.web.pages', //  GSP
		           'org.codehaus.groovy.grails.web.sitemesh', //  layouts
		           'org.codehaus.groovy.grails.web.mapping.filter', // URL mapping
		           'org.codehaus.groovy.grails.web.mapping', // URL mapping
		           'org.codehaus.groovy.grails.commons', // core / classloading
		           'org.codehaus.groovy.grails.plugins', // plugins
		           'org.codehaus.groovy.grails.orm.hibernate', // hibernate integration
		           'org.springframework',
		           'org.hibernate',
		           'net.sf.ehcache.hibernate'
		
		    warn   'org.mortbay.log'
					   
			debug  'grails.app.services.org.annotopia.grails.services.storage.jena.VirtuosoJenaStoreService',
				   'org.annotopia.groovy.service.store'
			
			trace  'grails.app' // Necessary for Bootstrap logging
		}
	}
	
	production {		
		grails.logging.jul.usebridge = false
		
		def catalinaBase = System.properties.getProperty('catalina.base')
		if (!catalinaBase) catalinaBase = '.'   // just in case
		def logDirectory = "${catalinaBase}/logs"
		
		log4j = {
			appenders {
				// Set up a log file in the standard tomcat area; be sure to use .toString() with ${}
				rollingFile name:'tomcatLog', threshold: org.apache.log4j.Level.INFO,  file:"${logDirectory}/"+appName+".log".toString(), maxFileSize:1024
			}
			
			root {
				// Change the root logger to my tomcatLog file
				info 'tomcatLog'
				additivity = true
			}
			
			info   'grails.app'
		
			error  'org.codehaus.groovy.grails.web.servlet',  //  controllers
				   'org.codehaus.groovy.grails.web.pages', //  GSP
				   'org.codehaus.groovy.grails.web.sitemesh', //  layouts
				   'org.codehaus.groovy.grails.web.mapping.filter', // URL mapping
				   'org.codehaus.groovy.grails.web.mapping', // URL mapping
				   'org.codehaus.groovy.grails.commons', // core / classloading
				   'org.codehaus.groovy.grails.plugins', // plugins
				   'org.codehaus.groovy.grails.orm.hibernate', // hibernate integration
				   'org.springframework',
				   'org.hibernate',
				   'net.sf.ehcache.hibernate'
		}
	}
}

cors.url.pattern = ['/s/annotation/*','/s/annotationset/*']
cors.headers = ['Access-Control-Allow-Origin':'*']

