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
-- Calculate boosts for captures based on combination of page-inlinks
-- and domain-inlinks.
--
%default INPUT  ''
%default OUTPUT ''

SET job.name '$JOBNAME'
SET mapred.output.compress 'true'

REGISTER lib/bacon.jar
REGISTER lib/json.jar

DEFINE FROMJSON org.archive.bacon.FromJSON();
DEFINE TOJSON   org.archive.bacon.ToJSON();

-- Define a macro for the Java Math.max() function.  Easier to use than the Pig MAX() function in this particular instance.
DEFINE maximum InvokeForString('java.lang.Math.max', 'double double'); 


data = LOAD '$INPUT' 
       USING org.archive.bacon.io.SequenceFileLoader() 
       AS ( key:chararray, value:chararray );

-- Convert the JSON strings into Pig Map objects
data = FOREACH data GENERATE FROMJSON(value) AS m:[];

-- We have to add the cast to long.  The JSON library does something
-- odd when deserializing a JSON object into Java-land.  For integer
-- numbers the JSON library will return an Integer if the value fits,
-- otherwise a Long.  Since we don't know which one it will be
-- (Integer or Long), we add the cast to Long and Pig takes care of it.
data = FOREACH data GENERATE m#'url'    AS url:chararray, 
       	       	    	     m#'digest' AS digest:chararray,
			     m#'domain' AS domain:chararray,
			     ((long)m#'page_inlink_count')   AS page_inlink_count:long,
			     ((long)m#'domain_inlink_count') AS domain_inlink_count:long;

-- Convert null values to '1' so that the calls to LOG10() don't freak out.
data = FOREACH data GENERATE url, digest, (page_inlink_count   is null ? 1L : page_inlink_count  ) as page_inlink_count,
       	       	    	     	  	  (domain_inlink_count is null ? 1L : domain_inlink_count) as domain_inlink_count;

-- Calculate boost for each page.
data = FOREACH data GENERATE url, digest, 1.0 + 
       	       	    	     	          (maximum( LOG10( (double) page_inlink_count   ), 0.0 ) /  2.0) + 
					  (maximum( LOG10( (double) domain_inlink_count ), 0.0 ) / 10.0)  
					  as boost:double;

-- Omit boost values of 1.0 since they have no effect.  Don't bother writing them into the output.
data = FILTER data BY boost > 1.0;

-- Formulate the data as a Map, so we can write it out in JSON form.
-- The format of the key must match that used in the parsed documents,
-- so that the boost value can be joined with the parsed document when
-- building the full-text index.
data = FOREACH data GENERATE CONCAT( url, CONCAT( ' ', digest ) ),
                             TOJSON( TOMAP( 'url',    url,
       	       	    	                    'digest', digest,
			                    'boost',  boost  ) );

STORE data INTO '$OUTPUT' USING org.archive.bacon.io.SequenceFileStorage();
