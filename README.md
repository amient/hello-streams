# 1. Wikipedia Demo


# 2. Recursive Web Crawler Demo

Web Crawler done as a recursive stream processor with Apache Flink

## Topology

                                         URL Processor
                                 +--------------------------+
                                 |                          |
                             +-> | ? > Fetch > Clean > Page |
                            /    |                          |
                           /     +--------------------------+
          ~~~~~~~~~~~~~~  /      |                          | - INPUT: URL
    +--->   URL Stream   +-----> | ? > Fetch > Clean > Page | -=STATE: Set of URLs
    |     ~~~~~~~~~~~~~~  \      |                          |   already requested
    |                      \     +--------------------------+ - OUTPUT: PAGE
    |                       \    |                          |
    |                        +-> | ? > Fetch > Clean > Page |
    |                            |                          |
    |                            +------------+-------------+
    |                                         |
    |                                         |
    |           +-----------------------------+
    |           |
    |           |                     PAGE Processor
    |           |                +---------------------+
    |           |                | ? Already Crawled   |
    |           |                |  > Extract Links    |
    |           |            +-> |   > Produce URLs    |
    |           v           /    |    > Produce Assets |  - INPUT: PAGE
    |     ~~~~~~~~~~~~~~~  /     |                     |  - STATE: Set of Content
    |       PAGE Stream   +      +---------------------+    Hashes already crawled
    |     ~~~~~~~~~~~~~~~  \     | ? Already Crawled   |  - OUTPUT 1: URLs       
    |                       \    |  > Extract Links    |  - OUTPUT 2: ASSETS
    |                        +-> |   > Produce URLs    |
    |                            |    > Produce Assets |
    |                            |                     |
    |                            +--------+-+----------+
    |                                     | |
    +-------------------------------------+ |
                                            |
                 +--------------------------+
                 |
                 |
                 v
         ~~~~~~~~~~~~~~~~
           ASSET Stream   +-------->   PRINT
         ~~~~~~~~~~~~~~~~
