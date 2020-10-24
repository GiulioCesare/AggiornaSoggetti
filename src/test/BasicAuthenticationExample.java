package test;


	/*
	 * $Header: 
	 * $Revision: 1.1 $
	 * $Date: 2017/01/13 13:41:50 $
	 * ====================================================================
	 *
	 *  Copyright 1999-2004 The Apache Software Foundation
	 *
	 *  Licensed under the Apache License, Version 2.0 (the "License");
	 *  you may not use this file except in compliance with the License.
	 *  You may obtain a copy of the License at
	 *
	 *      http://www.apache.org/licenses/LICENSE-2.0
	 *
	 *  Unless required by applicable law or agreed to in writing, software
	 *  distributed under the License is distributed on an "AS IS" BASIS,
	 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	 *  See the License for the specific language governing permissions and
	 *  limitations under the License.
	 * ====================================================================
	 *
	 * This software consists of voluntary contributions made by many
	 * individuals on behalf of the Apache Software Foundation.  For more
	 * information on the Apache Software Foundation, please see
	 * <http://www.apache.org/>.
	 *
	 * [Additional notices, if required by prior licensing conditions]
	 *
	 */
	 
	import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.PostMethod;
	 
	/**
	 * A simple example that uses HttpClient to perform a GET using Basic
	 * Authentication. Can be run standalone without parameters.
	 *
	 * You need to have JSSE on your classpath for JDK prior to 1.4
	 *
	 * @author Michael Becke
	 */
	public class BasicAuthenticationExample {
	     
	    /**
	     * Constructor for BasicAuthenticatonExample.
	     */
	    public BasicAuthenticationExample() {
	        super();
	    }
	     
	    public static void main(String[] args) throws Exception {
	        HttpClient client = new HttpClient();
	         
	        // pass our credentials to HttpClient, they will only be used for
	        // authenticating to servers with realm "realm" on the host
	        // "www.verisign.com", to authenticate against an arbitrary realm 
	        // or host change the appropriate argument to null.
	        client.getState().setCredentials(
	                new AuthScope("10.30.1.83", 80, AuthScope.ANY_REALM),
//	                new UsernamePasswordCredentials("XXXAMM000015", "import99")
//	                new UsernamePasswordCredentials("XXXAMM000076", "borrel60")
	                new UsernamePasswordCredentials("XXXAMM000001", "frapao")
	                
	                );
	         
	        // create a GET method that reads a file over HTTPS, 
	        // we're assuming that this file requires basic 
	        // authentication using the realm above.
//	        GetMethod get = new GetMethod("http://10.30.1.83/certificazione/serversbnmarc");
	        PostMethod post = new PostMethod("http://10.30.1.83/certificazione/servlet/serversbnmarc");
	         
	        // Tell the GET method to automatically handle authentication. The
	        // method will use any appropriate credentials to handle basic
	        // authentication requests.  Setting this value to false will cause
	        // any request for authentication to return with a status of 401.
	        // It will then be up to the client to handle the authentication.
//	        get.setDoAuthentication( true );
	        post.setDoAuthentication( true );
	        
	      	String requestXml = "<?xml version='1.0' encoding='UTF-8'?>"
	      			+ "<SBNMarc schemaVersion='2.02'><SbnUser><Biblioteca>XXXAMM</Biblioteca>"
	      			+ "<UserId>000001</UserId></SbnUser><SbnMessage><SbnRequest>"
	      			+ "<Cerca numPrimo='1' tipoOrd='1' tipoOutput='000'><CercaTitolo><CercaDatiTit><T001>"
	      			+ "BVE0000001"
	      			+ "</T001></CercaDatiTit></CercaTitolo></Cerca></SbnRequest></SbnMessage></SBNMarc>";
	        
	        
//	        get.ad setQueryString("testo_xml="+requestXml);
	        post.addParameter("testo_xml", requestXml);
	        
	        int status=0;;
	        try {
	            // execute the GET
//	            status = client.executeMethod( get );
	            status = client.executeMethod( post );
	             
	            // print the status and response
//	            System.out.println(status + "\n" + get.getResponseBodyAsString());
	            System.out.println(status + "\n" + post.getResponseBodyAsString());
	             
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
	        } finally {
	            // release any connection resources used by the method
//	            get.releaseConnection();
	            post.releaseConnection();
	            System.out.println("status: "+status);
	        }
	    }
	}
