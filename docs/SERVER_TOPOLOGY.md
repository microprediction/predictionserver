


### Server class dependencies 


          
          
      ObscurityHabits  
      |
      |             AttributeConventions    KeyConventions      LeaderboardConventions    SubscriptionConventions  MetricConventions
      |                       |                     |                      |                     |
      |    BalanceConventions |  HorizonConventions |   LaggedConventions  |    LinkConventions  |     MemoConventions |  <others....>  
      |       |               |         |           |      |               |            |        |             |       |
      |------>|>------AttributeHabits-->|>---- KeyHabits-->|>-------LeaderboardHabits-->|>-SubscriptionHabits->|>--MetricHabits
      |       |               |         |           |      |               |            |        |             |       |   
      ---- BalanceHabits------|---HorizonHabits---->|>--- LaggedHabits --->|>-------LinkHabits-->|>-------MemoHabits   |   <otherHabits>         
              |               |         |           |      |               |            |        |             |       |       |
              ------------------------------------------------------------------------------------------------------------------
                                              |
                                           Habits     (all public and also all private implementation conventions)
                                              |
                                          BaseServer
                                              |
                                --------------------------
                                |             |              
                            MemoServer  OwnershipServer   
                                |             |              
                                --------------------------
                                              |   
                   -------------------------------------------------------------                          
                   |              |                |               |            |  
                AttributeServer MetricServer HashSever  SortedSetServer  ListServer
                   |              |                |               |            |
                   -------------------------------------------------------------
                                             |
                                        ObjectServer 
                                             |       
           ------------------------------------------------------------------
           |              |                  |               |
     BalanceServer  PerformanceServer LeaderboardServer   LaggedServer                 
           |              |                  |               |          
           -----------------------------------------------------------------  
                                             |
                                      BaseStreamServer              
                                             |
                                    -------------------------------
                                    |         |                   |
                               LinkServer SubscriptionServer LaggedServer               
                                    |         |                   |
                                    -------------------------------
                                              |
                             -------------------------------------
                             |                |                  |
                        ScenarioServer    other triggered mechanisms                       
                             |                |                  | 
                             -------------------------------------                 
                                              |
                                         StreamServer                                            
                                              |
            ----------------------------------------------------------------
            |            |                                 |               | 
          CdfServer   RatingsServer -----------        PromiseDaemon    <others...> 
            |            |                    |            | 
            |         RatingsDaemon  RecommendationServer  |
            |            |                    |            | 
            |            |        RecommendationsDaemon    |
            |            |                    |            |
            |            |                    |            | 
            -------------|---------------------------------|
                                              |
                                          GarbageDaemon    
                                              |
                                          MicroServer
                                                 
                                          
  
                
                