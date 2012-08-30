Building the graph with Common Crawl corpus
==============

This sub project is dedicated to extract and gather all the elements to draw the OpenData graph
It is a maven project which builds up a jar and bundles all the dependencies inside to be executed on AWS EMR via the web console

Important
---------
To successfully build and run this project, you should modify the _src/main/java/aws.properties_ file and put your AWS credentials and your S3 bucket name

It is composed of a set of MapReduce jobs which depend on other utils classes and represents the *3* steps of the construction of the OpenData graph
These steps (not an EMR step) are executed one after another in the following order

French Web Indexing and French OpenData web Indexing
----------------
This step aims to build an index of all the French websites and all their locations on CommonCrawl corpus
The corresponding job outputs a full index for all the french websites and the French OpenData websites index in addition.
*Package* : _com.datapublica.commoncrawl.indexing_

OpenData linked graph constructor
-----------
The graph is a set of nodes which are the websites names and edges which are a set of < Source , Destination > pairs.
This step aims to reveal all links the between OpenData sites. To do so it uses the metadata files of common crawl to find the links between these websites.
This step depends on the previous one and uses its output of paths to process only the files that might contain OpenData pages.
*Package* : _com.datapublica.commoncrawl.linking_

Open data stats
--------------
This part focuses on the building of a clean list of OpenData sites based on some page count statistics. We have studied these stats a little bit and determined thresholds to define an OpenData site
*Package* : _com.datapublica.commoncrawl.stats.opendata_

Licence
------------------
All software in this repository is LICENSED under BSD Licence : 

** Copyright (c) 2012, Data Publica
** All rights reserved.
** Redistribution and use in source and binary forms, with or without
** modification, are permitted provided that the following conditions are met:
*
**     * Redistributions of source code must retain the above copyright
**       notice, this list of conditions and the following disclaimer.
**     * Redistributions in binary form must reproduce the above copyright
**       notice, this list of conditions and the following disclaimer in the
**       documentation and/or other materials provided with the distribution.
**     * Neither the name of Data Publica nor the
**       names of its contributors may be used to endorse or promote products
**       derived from this software without specific prior written permission.
*
** THIS SOFTWARE IS PROVIDED BY DATA PUBLICA AND CONTRIBUTORS ``AS IS'' AND ANY
** EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
** WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
** DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
** DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
** (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
** LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
** ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
** (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
** SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

