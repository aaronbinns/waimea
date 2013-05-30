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
-- Perform url-agnostic deduplication of records by grouping records
-- based on their digest value, then munging all the document
-- metadata, codes and dates around it.  In the end, split captures
-- back out by (url,digest).
--
%default INPUT  ''
%default OUTPUT ''

SET job.name '$JOBNAME'
SET mapred.output.compress 'true'
SET mapred.job.reuse.jvm.num.tasks 1

REGISTER lib/bacon.jar
REGISTER lib/json.jar

DEFINE TRUNCATE org.archive.bacon.Truncate();
DEFINE FROMJSON org.archive.bacon.FromJSON();
DEFINE TOJSON   org.archive.bacon.ToJSON();

-- Load the captures
captures  = LOAD '$INPUT' 
            USING org.archive.bacon.io.SequenceFileLoader() 
            AS ( key:chararray, value:chararray );

-- Convert the JSON strings into Pig Map objects.
captures = FOREACH captures GENERATE FROMJSON( value ) AS m:[];

-- Unwind the map into one big relation with each map key/value pair
-- as a field in the relation.
captures = FOREACH captures GENERATE m#'url'         AS url         :chararray,
                                     m#'digest'      AS digest      :chararray,
                                     m#'title'       AS title       :chararray,
                                     m#'keywords'    AS keywords    :chararray,
                                     m#'description' AS description :chararray,
                                     m#'length'      AS length      :chararray,
                                     m#'code'        AS code        :chararray,
                                     m#'content'     AS content     :chararray,
                                     m#'boiled'      AS boiled      :chararray,
                                     m#'type'        AS type        :chararray,
                                     m#'date'        AS date        :chararray;

-- From all the captures, extract the 'documents', meaning the
-- captures that have a non-empty 'content' field.  Revisit records
-- won't have a 'content' field.  We retain the 'date' value here so
-- that we can sort the documents by capture date.
documents = FOREACH captures GENERATE digest, 
                                      title, 
                                      keywords, 
                                      description, 
                                      length, 
                                      code,
                                      TRUNCATE(content,100000) as content:chararray,
                                      TRUNCATE(boiled ,100000) as boiled:chararray,
                                      type,
                                      date
                                      ;

documents = FILTER documents BY (content != '' and content is not null);

-- Collate the list of unique dates for each capture.
dates = FOREACH captures GENERATE digest, url, date;
dates = FILTER dates BY (date != '' and date is not null);

dates = GROUP dates BY (url,digest);
dates = FOREACH dates GENERATE FLATTEN(group) AS (url,digest), dates.date AS dates;
dates = FOREACH dates 
        {
          dates = DISTINCT dates;
          GENERATE url, digest, dates;
        }

-- Group everything back together by digest.
captures = GROUP documents BY digest ,
                 dates     BY digest 
                 ;

-- Only retain the most recent document.
captures = FOREACH captures
            {
              documents = ORDER documents BY date DESC;
              document  = LIMIT documents 1;
              dates = dates.(url,dates);
              GENERATE FLATTEN(document), FLATTEN(dates);
            };

-- Put the data into a Map and store it in JSON format.
-- The format of the key must match that used in the parsed documents,
-- so that the boost value can be joined with the parsed document when
-- building the full-text index.
captures = FOREACH captures GENERATE CONCAT( url, CONCAT( ' ', digest ) ),
                                     TOJSON( TOMAP( 'url',         url,
                                                    'digest',      digest,
                                                    'title',       title,
                                                    'keywords',    keywords,
                                                    'description', description,
                                                    'length',      length,
                                                    'code',        code,
                                                    'content',     content,
                                                    'boiled',      boiled,
                                                    'type',        type,
                                                    'date',        dates ) );

STORE captures INTO '$OUTPUT' USING org.archive.bacon.io.SequenceFileStorage();
