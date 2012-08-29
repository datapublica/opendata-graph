opendata-graph
==============

This project aims to crawl Common Crawl corpus in order to create a graph of french opendata websites

**NB**: *this project is not yet finished, and will continue to evolve in the next few days.*

French open-data websites Graph
-------------------------------
Our project aims to build a subgraph of the web, consisting of the French websites mentioning open-data. This graph enables viewers to see popular websites and connections between them, to see which kind of entities communicate with the others (Companies, Non profits/Blogs, Government agencies). It is a good way to discover the actors of the French open-data, and how they relate to one another.

Crawl Methodology
-----------------
We crawled the whole Common Crawl corpus. For each website, we computed two scores: an open-data score, and a "French" score. If both are large enough, the website is kept to build the graph, together with all its outgoing links (which we use to build edges of the graph). Once the crawl is over and the websites selected, two files are generated: one with the nodes and one with the connections between the nodes.

**Important** : to build Java Project (in commonCrawl directory), you need to complete a configuration file with your Amazon information, located on _commonCrawl/src/main/resources/aws.properties_

Categorization
--------------
The nodes are then manually grouped by categories: type (Companies, Non profits/Blog, Government agencies) and roles (Open-Data Speaker, Open-Data Dealer).

Visualization
-------------
The resulting dataset is then loaded into Gephi, to be spatialized and visualized.

Graph view (draft)
------------------
If you want to see some pictures of the draft Graph realized, **you can go into the preview directory**. You'll have access to an overview, an image of the core, and images of two mini-clusters separated from the main graph.
This preview will be soon completed with a dynamic general view of the graph, with categorized nodes, and a complete analysis of the results.

