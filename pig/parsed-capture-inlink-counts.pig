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
-- Join the list of parsed captures with the list of all page inlink
-- counts to produce the inlink counts for the parsed pages.
--
-- The output of this script is suitable as input to parsed-capture-boost.pig
--
%default PARSED_CAPTURES      ''
%default INLINK_COUNTS_PAGE   ''
%default INLINK_COUNTS_DOMAIN ''
%default OUTPUT ''

SET job.name '$JOBNAME'
SET mapred.output.compress 'true'

REGISTER lib/bacon.jar
REGISTER lib/json.jar

DEFINE FROMJSON org.archive.bacon.FromJSON();
DEFINE TOJSON   org.archive.bacon.ToJSON();

parsed_captures      = LOAD '$PARSED_CAPTURES'      USING org.archive.bacon.io.SequenceFileLoader() AS ( key:chararray, value:chararray );
page_inlink_counts   = LOAD '$INLINK_COUNTS_PAGE'   USING org.archive.bacon.io.SequenceFileLoader() AS ( key:chararray, value:chararray );
domain_inlink_counts = LOAD '$INLINK_COUNTS_DOMAIN' USING org.archive.bacon.io.SequenceFileLoader() AS ( key:chararray, value:chararray );

parsed_captures      = FOREACH parsed_captures      GENERATE FROMJSON( value ) AS m:[];
page_inlink_counts   = FOREACH page_inlink_counts   GENERATE FROMJSON( value ) AS m:[];
domain_inlink_counts = FOREACH domain_inlink_counts GENERATE FROMJSON( value ) AS m:[];

parsed_captures      = FOREACH parsed_captures      GENERATE m#'url'    AS url:chararray,    m#'digest' AS digest:chararray, m#'domain' AS domain:chararray;
page_inlink_counts   = FOREACH page_inlink_counts   GENERATE m#'url'    AS url:chararray,    m#'num' AS num:long;
domain_inlink_counts = FOREACH domain_inlink_counts GENERATE m#'domain' AS domain:chararray, m#'num' AS num:long;

data = JOIN parsed_captures BY url LEFT, page_inlink_counts BY url;
data = FOREACH data GENERATE parsed_captures::url    AS url, 
       	       	    	     parsed_captures::digest AS digest,
       	       	    	     page_inlink_counts::num AS page_inlink_count, 
			     parsed_captures::domain AS domain;

data = JOIN data BY domain LEFT, domain_inlink_counts BY domain;
data = FOREACH data GENERATE data::url                 AS url, 
       	       	    	     data::digest              AS digest,
       	       	    	     data::page_inlink_count   AS page_inlink_count, 
			     data::domain              AS domain, 
			     domain_inlink_counts::num AS domain_inlink_count;

data = FOREACH data GENERATE TOJSON( TOMAP( 'url',                 url,
       	       	    	             	    'digest',              digest,
				            'page_inlink_count',   page_inlink_count,
				            'domain',              domain,
				            'domain_inlink_count', domain_inlink_count ) );

STORE data INTO '$OUTPUT' USING org.archive.bacon.io.SequenceFileStorage();
