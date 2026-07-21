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
        subgraph data
            disruptor
            subgraph transception
                direction LR
                mqtt
                grafana
            end

            subgraph storage
                direction LR
                redis
                native
            end

            subgraph types
                direction LR
                subgraph message
                    direction LR
                    bbo_update
                    trade_update
                end
            end
        end

        subgraph data_provider
            direction LR
            hyperliquid_wss
            polymarket_wss
            betfair_wss
            binance_wss
            dydx_wss
        end
        
    end

    data_provider --> message
    data --> transception

    message --> disruptor
    disruptor --> execution
    disruptor --> storage
    storage --> execution

    mqtt --> grafana

    disruptor --> transception

    execution_logic --> executor
```

### dependency graph
```mermaid
flowchart TD
    subgraph dependency_graph
        config --> strategy
        config --> exchange

        strategy --> exchange
        strategy --> data

        exchange --> data

        data --> types
        data --> transception
        data --> storage

        storage --> redis
    end
```

> [!tip]
> use `cargo test` to verify that there are no circular dependencies

### services
```mermaid
flowchart TD
    mma <--> redis
    mma --> mqtt
    mqtt --> grafana
    mma <--> pgsql
    pgsql --> grafana
```

### strategies
#### avellaneda stoikov market making
```mermaid
stateDiagram-v2
    watch_trades --> σ
    watch_trades --> κ
    watch_trades --> q
    
    
    κ --> optimal_spread
    σ --> optimal_spread 
    
    
    q --> reservation_price
    σ --> reservation_price
    γ --> reservation_price

    reservation_price --> create_update_order
    optimal_spread --> create_update_order
    create_update_order --> watch_trades
```