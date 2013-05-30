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
-- Process the *inter-domain* links, producing 2 reports:
--
-- 1 'inlink-counts-page'
--      For each page, uniq # of inlinks to that page
--
-- 2 'inlink-counts-domain'
--      For each domain, uniq # of other domains with a link to it.
--
--
%default LINKS  ''
%default INLINK_COUNTS_PAGE   ''
%default INLINK_COUNTS_DOMAIN ''

SET job.name '$JOBNAME'
SET mapred.output.compress 'true'

REGISTER lib/bacon.jar
REGISTER lib/json.jar

DEFINE FROMJSON org.archive.bacon.FromJSON();
DEFINE TOJSON   org.archive.bacon.ToJSON();

-- Load the inter-site links.
links = LOAD '$LINKS' 
        USING org.archive.bacon.io.SequenceFileLoader() 
        AS ( key:chararray, value:chararray );

links = FOREACH links GENERATE FROMJSON( value ) AS m:[];

links = FOREACH links GENERATE m#'src'         AS src:chararray,
      		      	       m#'src_host'    AS src_host:chararray,
			       m#'src_domain'  AS src_domain:chararray,
			       m#'dest'        AS dest:chararray,
      		      	       m#'dest_host'   AS dest_host:chararray,
			       m#'dest_domain' AS dest_domain:chararray;

links = FOREACH links GENERATE dest, dest_domain, src_domain;

-- Count the number of unique pages that link to this page.  Due to
-- the logic in link-graph.pig which produces the input to this
-- script, we know that the inlinking pages are already unique.
pages = FOREACH links GENERATE dest;
pages = GROUP pages BY dest;
pages = FOREACH pages GENERATE group AS dest, COUNT(pages) AS num:long;

-- Count the number of unique domains that link to this domain.
domains = FOREACH links GENERATE dest_domain, src_domain;
domains = GROUP domains BY dest_domain;
domains = FOREACH domains 
	  {
	    src_domains = domains.src_domain;
	    src_domains = DISTINCT src_domains;
	    GENERATE group AS dest_domain, COUNT(src_domains) AS num:long;
	  }

-- Write out the page and domain counts.
pages   = FOREACH pages   GENERATE TOJSON( TOMAP( 'url', dest, 
                                                  'num', num ) );

domains = FOREACH domains GENERATE TOJSON( TOMAP( 'domain', dest_domain, 
                                                  'num',    num ) );

STORE pages   INTO '$INLINK_COUNTS_PAGE'   USING org.archive.bacon.io.SequenceFileStorage();
STORE domains INTO '$INLINK_COUNTS_DOMAIN' USING org.archive.bacon.io.SequenceFileStorage();
