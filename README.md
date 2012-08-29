opendata-graph
==============

This project aims to crawl Common Crawl corpus in order to create a graph of french opendata websites

**NB**: *this project is not yet finished, and will continue to evolve in the next few days.*

French open-data websites Graph
-------------------------------
Our project aims to build a subgraph of the web, made of the french websites speaking about open-data. This graph enables viewers to see connections between websites, which websites are the most populars, and wich kind of entities communicate with the others (Companies, associations/citizen, State). It is a good way to discover which are the actors of the french open-data, and how they organize together.

Crawl Methodology
-----------------
We crawled the whole Common Crawl corpus, and for each website we built two scores : an open-data score, and a "french" score. If both are big enough, the website is kept to build the graph, with all its outgoing links (which are used to build edges of the graph). One the crawl is over and the websites selected, two files are generated : the first one contains the nodes, the second one contains the edges (which node is connected ti which one).

Categorization
--------------
The nodes are then categorized and qualified : type (Companies, associations/citizen, State) and function (Open-Data Speaker, Open-Data Dealer).

Visualization
-------------
The resulting dataset is then loaded into Gephi, to be spatialized and visualized.

Graph view (draft)
------------------
If you want to see some pictures of the draft Graph realized, **you can go into the preview repository**. You'll have access to an overview, an image of the core, and images of two mini-clusters separated from the main graph.
This preview will be soon completed with a dynamic general view of the graph, with categorized nodes, and a complete analysis of the results.

