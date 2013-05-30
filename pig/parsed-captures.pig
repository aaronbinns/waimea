/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

--
-- Generate some handy metadata for the unique parsed captures.
--
%default INPUT  ''
%default OUTPUT ''

SET job.name '$JOBNAME'
SET mapred.output.compress 'true'

REGISTER lib/bacon.jar
REGISTER lib/json.jar

DEFINE FROMJSON org.archive.bacon.FromJSON();
DEFINE TOJSON   org.archive.bacon.ToJSON();
DEFINE HOST     org.archive.bacon.url.Host();
DEFINE DOMAIN   org.archive.bacon.url.Domain();

-- Load the metadata from the parsed data, which is JSON strings stored in a Hadoop SequenceFile.
captures  = LOAD '$INPUT' 
            USING org.archive.bacon.io.SequenceFileLoader()
            AS ( key:chararray, value:chararray );

-- Convert the JSON strings into Pig Map objects.
captures = FOREACH captures GENERATE FROMJSON( value ) AS m:[];

-- Only retain records where the errorMessage is not present.  Records
-- that failed to parse will be present in the input, but will have an
-- errorMessage property, so if it exists, skip the record.
captures = FILTER captures BY m#'errorMessage' is null;

-- Only retain the fields needed here.
captures = FOREACH captures GENERATE m#'url'    AS url:chararray,
			    	     m#'digest' AS digest:chararray;

-- Discard duplicates
captures = DISTINCT captures;

-- Get the host from the url and filter out with an empty host.
captures = FOREACH captures GENERATE url, digest, HOST(url) AS host:chararray;
captures = FILTER captures BY host != '';

-- Compute the domain.
captures = FOREACH captures GENERATE url, digest, host, DOMAIN(host) as domain:chararray;

-- Sometimes we cannot calculate the domain, for example, a URL that
-- uses an IP address instead of hostname, http://192.168.1.1/foo.html
-- Another example may be captures from very old archives which had
-- different domain/publicsuffix rules.  In those cases, we just use
-- the hostname as the domain and hope for the best.
captures = FOREACH captures GENERATE url, digest, host, ((domain != '') ? domain : host) as domain;

captures = FOREACH captures GENERATE TOJSON( TOMAP( 'url',    url, 
	   	   	    	     	            'digest', digest,
				   	            'host',   host,
				   	            'domain', domain ) );

STORE captures INTO '$OUTPUT' USING org.archive.bacon.io.SequenceFileStorage();
