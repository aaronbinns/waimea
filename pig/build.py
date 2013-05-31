# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Import Pig, Hadoop and Java classes used in this script.
from org.apache.pig.scripting import Pig
from org.apache.hadoop.conf import Configuration
from org.apache.hadoop.fs import FileSystem
from org.apache.hadoop.fs import Path
from org.apache.hadoop.util import RunJar
from org.apache.pig.backend.hadoop.executionengine.mapReduceLayer import RunJarSecurityManager
from java.io import IOException
from java.util import Properties
from java.lang import SecurityException
from java.lang import System

import sys
import time

startTime = time.time()

if len(sys.argv) != 2:
    raise sys.argv[0] + ' <basedir>'

# Get reference to the Hadoop FileSystem object.  Everything we do in
# this script that interacts with HDFS is through this object.
fs = FileSystem.get(Configuration())

# Make sure the requested collection exists.
collection = sys.argv[1]
collectionDir = Path( collection )

if not fs.exists( collectionDir ):
    print '\nERROR: no collection directory: %s' % collectionDir
    System.exit(1)

# Check for "guard" file.  Like a semaphore, ensures that we don't try
# to update this collection while it's in the middle of being updated.
# Since file creation in HDFS is atomic, we don't check for the existence
# of the guardFile, rather we try to create it.  If the file already exists
# then fs.createNewFile() will return False
guardFile = Path( collectionDir, '_updating' )
if not fs.createNewFile( guardFile ):
    print '\nERROR: collection update already in progress: %s' % guardFile
    System.exit(1)

# Setup paths that are used later in the script.
incomingDir = Path( collectionDir, 'incoming'   )
warcsDir    = Path( collectionDir, 'warcs'      )
noindexFile = Path( collectionDir, 'noindex'    )
parsedDir   = Path( collectionDir, 'parsed'     )
indexesDir  = Path( collectionDir, 'indexes'    )
dedup       = Path( collectionDir, 'dedup'      )
# More paths, for the graph-related info
parsedCaptures            = Path( collectionDir, 'graph/parsed-captures'      )
linkGraph                 = Path( collectionDir, 'graph/link-graph'           )
inlinkCountsPage          = Path( collectionDir, 'graph/inlink-counts-page'   )
inlinkCountsDomain        = Path( collectionDir, 'graph/inlink-counts-domain' )
parsedCaptureInlinkCounts = Path( collectionDir, 'graph/parsed-capture-inlink-counts' )
parsedCaptureBoost        = Path( collectionDir, 'graph/parsed-capture-boost'         )

# Properties used for all jobs
props = Properties()
props.put('mapred.fairscheduler.pool', 'default')

# **************************************************
# Move any (w)arc files from 'incoming/' to 'arcs/'.
incoming = fs.globStatus( Path( incomingDir, '*.gz' ) )

print 'LOG: Incoming %d' % len(incoming)
if len(incoming) > 0:
    if not fs.exists( warcsDir ):
        # print 'DEBUG: Bootstrap warcs dir: %s' % warcsDir
        fs.mkdirs( warcsDir )

    for newfile in incoming:
        # It's not unusual for (w)arc files to be put into the incoming directory
        # a second, third, etc. time.  The "ingest pipeline" can have various troubles
        # and the general approach is to re-run the file through the pipeline until
        # it goes all the way through.  Since one of the pipeline steps is to push
        # the file into HDFS, it can be pushed in multiple times.
        #
        # So, we check to see if the file is already in the 'warcsDir', and if it
        # is, we remove it from the 'incomingDir'.
        if fs.exists( Path( warcsDir, newfile.getPath().getName() ) ):
            print 'WARN: Incoming already exists, remove: %s' % newfile.getPath()
            fs.delete( newfile.getPath(), False );
            continue
        
        # Now that we've checked for any redundant files in the 'incomingDir', if
        # we cannot move the file then it is a serious problem.
        if not fs.rename( newfile.getPath(), warcsDir ):
            print '\nERROR: failed to move: %s -> %s' % (newfile.getPath(), warcsDir)
            System.exit(1)

# **************************************************
# If there are new warcs, parse 'em!
#
# We don't pass in an explicit list of warcs to the Parse job.  The
# Parse job takes the warcs directory as input and checks each one to
# see if it's already been parsed or not.
if fs.exists( warcsDir ):
        
    print 'LOG: Parse %d' % len( fs.globStatus( Path( warcsDir, '*.gz' ) ) )

    # Execute the Parse MapRecude job.  We crib some logic from
    # Pig's code that runs the MAPREDUCE Pig command, which uses
    # Pig's custom RunJarSecurityManager to trap the System.exit()
    # call at the end of most Hadoop jobs.
    securityManager = RunJarSecurityManager()
    try:
        # TODO: Add -q or -v option to Parse in JBs to supress the
        # chattiness if files are already parsed.
        print str(parsedDir)
        print str(warcsDir) + '/*.gz'
        RunJar.main( [ 'lib/jbs.jar', 
                       'org.archive.jbs.Parse',
                       '-Dmapred.fairscheduler.pool=default',
                       '-conf', 'etc/job-parse.xml',
                       str(parsedDir),
                       str(warcsDir) + '/*.gz' ] )
    except SecurityException:
        securityManager.retire()
    except IOException:
        securityManager.retire()
        print '\nERROR: Parsing failed for ' + collection
        # If the indexing job failed, remove any partially-completed parts.
        fs.delete( indexesDir, True )
        System.exit(1)

# Check for a "no index" file, which means that we skip the rest of
# the indexing work.  We check for it here, rather than at the very
# start because we want the files to be parsed, even if we don't index
# them.
if fs.exists( noindexFile ):
    print 'LOG: Skipping due to existance of no-index file: %s' % noindexFile
    endTime = time.time()
    print 'LOG: Elapsed %f' % (endTime - startTime)
    # Remove the guardFile
    fs.delete( guardFile, True )
    System.exit(0)

if fs.exists( parsedDir ):

    # parsed-captures
    if ( not fs.exists( parsedCaptures) or
         fs.getFileStatus( parsedDir ).getModificationTime() > fs.getFileStatus( parsedCaptures ).getModificationTime() ):
        print 'LOG: Graph parsed-captures create'
        fs.delete( parsedCaptures, True )
        params = { 'INPUT'  : str(parsedDir),
                   'OUTPUT' : str(parsedCaptures),
                   'JOBNAME': str(collection) + ' parsed-captures' }
        job = Pig.compileFromFile( 'pig/parsed-captures.pig' ).bind( params )
        result = job.runSingle(props)
        if not result.isSuccessful():
            print '\nERROR: Pig job parsed-captures for ' + collection
            System.exit(1)
    else:
        print 'LOG: Graph parsed-captures up-to-date'

    # link-graph
    if ( not fs.exists( linkGraph ) or
         fs.getFileStatus( parsedDir ).getModificationTime() > fs.getFileStatus( linkGraph ).getModificationTime() ):
        print 'LOG: Graph link-graph create'
        fs.delete( linkGraph, True )
        params = { 'INPUT'  : str(parsedDir),
                   'OUTPUT' : str(linkGraph),
                   'JOBNAME': str(collection) + ' link-graph' }
        job = Pig.compileFromFile( 'pig/link-graph.pig' ).bind( params )
        result = job.runSingle(props)
        if not result.isSuccessful():
            print '\nERROR: Pig job link-graph for ' + collection
            System.exit(1)
    else:
        print 'LOG: Graph link-graph up-to-date'

    # inlink-counts
    if ( not fs.exists( inlinkCountsPage   ) or 
         not fs.exists( inlinkCountsDomain ) or 
         fs.getFileStatus( linkGraph ).getModificationTime() > fs.getFileStatus( inlinkCountsPage   ).getModificationTime() or 
         fs.getFileStatus( linkGraph ).getModificationTime() > fs.getFileStatus( inlinkCountsDomain ).getModificationTime() ):
        print 'LOG: Graph inlink-counts create/update'
        fs.delete( inlinkCountsPage,   True )
        fs.delete( inlinkCountsDomain, True )
        params = { 'LINKS'                : str(linkGraph),
                   'INLINK_COUNTS_PAGE'   : str(inlinkCountsPage),
                   'INLINK_COUNTS_DOMAIN' : str(inlinkCountsDomain),
                   'JOBNAME'              : str(collection) + ' inlink-counts' }
        job = Pig.compileFromFile( 'pig/inlink-counts.pig' ).bind( params )
        result = job.runSingle(props)
        if not result.isSuccessful():
            print '\nERROR: Pig job inlink-counts for ' + collection
            System.exit(1)
    else:
        print 'LOG: Graph inlink-counts up-to-date'

    # parsed-capture-inlink-counts
    if ( not fs.exists( parsedCaptureInlinkCounts ) or
         fs.getFileStatus( parsedCaptures     ).getModificationTime() > fs.getFileStatus( parsedCaptureInlinkCounts ).getModificationTime() or
         fs.getFileStatus( inlinkCountsPage   ).getModificationTime() > fs.getFileStatus( parsedCaptureInlinkCounts ).getModificationTime() or
         fs.getFileStatus( inlinkCountsDomain ).getModificationTime() > fs.getFileStatus( parsedCaptureInlinkCounts ).getModificationTime() ):
        print 'LOG: Graph parsed-capture-inlink-counts create/update'
        fs.delete( parsedCaptureInlinkCounts, True )
        params = { 'PARSED_CAPTURES'     : parsedCaptures,
                   'INLINK_COUNTS_PAGE'  : inlinkCountsPage,
                   'INLINK_COUNTS_DOMAIN': inlinkCountsDomain,
                   'OUTPUT'              : parsedCaptureInlinkCounts,
                   'JOBNAME'             : str(collection) + ' parsed-capture-inlink-counts'  }
        job = Pig.compileFromFile( 'pig/parsed-capture-inlink-counts.pig' ).bind( params )
        result = job.runSingle(props)
        if not result.isSuccessful():
            print '\nERROR: Pig job parsed-capture-inlink-counts for ' + collection
            System.exit(1)
    else:
        print 'LOG: Graph parsed-capture-inlink-coounts up-to-date'

    # parsed-capture-boost
    if ( not fs.exists( parsedCaptureBoost ) or
         fs.getFileStatus( parsedCaptureInlinkCounts ).getModificationTime() > fs.getFileStatus( parsedCaptureBoost ).getModificationTime() ):
        print 'LOG: Graph parsed-capture-boost create/update'
        fs.delete( parsedCaptureBoost, True )
        params = { 'INPUT'  : parsedCaptureInlinkCounts,
                   'OUTPUT' : parsedCaptureBoost,
                   'JOBNAME': str(collection) + ' parsed-capture-boost'  }
        job = Pig.compileFromFile( 'pig/parsed-capture-boost.pig' ).bind( params )
        result = job.runSingle(props)
        if not result.isSuccessful():
            print '\nERROR: Pig job parsed-capture-boost for ' + collection
            System.exit(1)
    else:
        print 'LOG: Graph parsed-capture-boost up-to-date'

    # Dedup: perform url-agnostic deduplication of captures
    if ( not fs.exists( dedup ) or
         fs.getFileStatus( parsedDir ).getModificationTime() > fs.getFileStatus( dedup ).getModificationTime() ):
        print 'LOG: Deduplicate captures'
        fs.delete( dedup, True )
        params = { 'INPUT'  : parsedDir,
                   'OUTPUT' : dedup,
                   'JOBNAME': str(collection) + ' dedup' }
        job = Pig.compileFromFile( 'pig/dedup.pig' ).bind( params )
        result = job.runSingle(props)
        if not result.isSuccessful():
            print '\nERROR: Pig job dedup for ' + collection
            System.exit(1)
    else:
        print 'LOG: Deduplicated content up-to-date'

    # Index
    if ( not fs.exists( indexesDir ) or
         fs.getFileStatus( dedup              ).getModificationTime() > fs.getFileStatus( indexesDir ).getModificationTime() or
         fs.getFileStatus( parsedCaptureBoost ).getModificationTime() > fs.getFileStatus( indexesDir ).getModificationTime() ):
        print 'LOG: Index create %d' % len( fs.globStatus( Path( parsedDir, '*.gz' ) ) )
        fs.delete( indexesDir, True )
        securityManager = RunJarSecurityManager()
        try:
            RunJar.main( [ 'lib/jbs.jar', 
                           'org.archive.jbs.Merge',
                           '-conf', 'etc/job-index.xml',
                           '-Dmapred.fairscheduler.pool=default',
                           '-Djbs.lucene.collection=' + collection,
                           str(indexesDir),
                           str(parsedCaptureBoost),
                           str(dedup) ] )
        except SecurityException:
            securityManager.retire()
        except IOException:
            securityManager.retire()
            print '\nERROR: Indexing failed for ' + collection
            # If the indexing job failed, remove any partially-completed parts.
            fs.delete( indexesDir, True )
            System.exit(1)
    else:
        print 'LOG: Index up-to-date'


# Woo-hoo, all the updating steps completed successfully!

endTime = time.time()

print 'LOG: Elapsed %f' % (endTime - startTime)

# Last, and don't forget, remove the guardFile
fs.delete( guardFile, True )
