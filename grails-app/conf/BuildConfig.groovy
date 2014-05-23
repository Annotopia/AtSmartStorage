grails.project.class.dir = "target/classes"
grails.project.test.class.dir = "target/test-classes"
grails.project.test.reports.dir = "target/test-reports"

grails.project.dependency.resolution = {
    // inherit Grails' default dependencies
    inherits("global") {
        // uncomment to disable ehcache
        // excludes 'ehcache'
		excludes 'slf4j-log4j12'
    }
    log "warn" // log level of Ivy resolver, either 'error', 'warn', 'info', 'debug' or 'verbose'
    legacyResolve false // whether to do a secondary resolve on plugin installation, not advised and here for backwards compatibility
    repositories {
        grailsCentral()
        // uncomment the below to enable remote dependency resolution
        // from public Maven repositories
        //mavenLocal()
        mavenCentral()
        //mavenRepo "http://snapshots.repository.codehaus.org"
        //mavenRepo "http://repository.codehaus.org"
        //mavenRepo "http://download.java.net/maven/2/"
        //mavenRepo "http://repository.jboss.com/maven2/"
		
		//mavenRepo "http://build.mygrid.org.uk/maven/repository/"
		mavenRepo "https://repository.apache.org/content/repositories/snapshots/"
    }
    dependencies {
        // specify dependencies here under either 'build', 'compile', 'runtime', 'test' or 'provided' scopes eg.
		
		runtime 'virtuoso:virtjdbc:4'
		runtime 'virtuoso.sesame:virt_jena:2'

		compile ("org.apache.jena:jena-core:2.11.2-SNAPSHOT") {
			excludes 'slf4j-api', 'xercesImpl'
		}
		compile ("org.apache.jena:jena-arq:2.11.2-SNAPSHOT")
		ile ("xml-apis:xml-apis:1.4.01") {
			excludes 'xercesImpl'
		}
		test "org.spockframework:spock-grails-support:0.7-groovy-2.0"
		
		compile("xerces:xercesImpl:2.9.1") {
			excludes 'xml-apis'
			}
		 
		 
    }

    plugins {
        build(":tomcat:$grailsVersion",
              ":release:2.2.0",
              ":rest-client-builder:1.0.3") {
            export = false
        }	
		test(":spock:0.7") {
			exclude "spock-grails-support"
		}
		runtime ":cors:1.1.4"
    }
}
