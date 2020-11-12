
                                                            
     BalanceConventions  ...<others>   KeyConventions      HorizonConventions   DefaultUrls
           |                              |                    |                   |
           |                              |                    |                   |
           |                              |                    |                   |
           --------------------   Conventions --------------------------------------          
                                      |
                                      |
                                  BaseReader
                                      |
                                      |
     LeaderboardReader  MemoReader  ... <others> ....  AttributeReader      BalanceReader
            |             |           |                       |                  |  
            ----------------------------------------------------------------------             
                                      |
                                  MicroReader
                                      |
            ---------------------------------------------------------------------
            |                 |                 |             
     LeaderboardWriter    StreamWriter      AttributeWriter   
            |                 |                 |
            -------------------------------------               
                                      |
                                  MicroWriter
                                      |
                                      |--------------------|
                                      |                    |
                                  MicroCrawler         MicroPoll
                                      |
                                      |       
             