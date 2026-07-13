# sbOogway's market making arbitrage

## about
framework for market making and arbitrage on various exchanges and assets

## docs
https://docs.rs/mma/latest/mma/index.html

## architecture
### data flow
```mermaid
flowchart TD
    subgraph strategy
        
        subgraph execution [execution]
            direction LR
            subgraph executor [executor]
            end
            subgraph execution_logic [execution_logic]
            end
            
        end
        subgraph common_data_representation
            disruptor
            subgraph visualization
                direction LR
                mqtt
                grafana
            end

            subgraph message
                direction LR
            

                bbo_update
                trade_update
            end

            

        end

        subgraph data_provider
            direction LR
            hyperliquid_wss
            polymarket_wss
            betfair_wss
            binance_wss
        end

        

        
    end

    data_provider --> message
    common_data_representation --> visualization

    message --> disruptor
    disruptor --> execution

    mqtt <--> grafana

    disruptor --> visualization

    execution_logic --> executor
```

### dependency graph
```mermaid
flowchart TD
    subgraph dependency_graph
        config --> strategy

        strategy --> exchange
        strategy --> common_data_representation

        exchange --> data_provider
        exchange --> executor
        exchange --> message

        common_data_representation --> disruptor
        common_data_representation --> turso_db
        common_data_representation --> message
    end
```

> [!tip]
> use `cargo test` to verify that there are no circular dependencies

### strategies
#### avellaneda stoikov market making
```mermaid
flowchart TD
    subgraph asmm
        subgraph state
            ask_price
            bid_price
            mid_price

        end
    end

```