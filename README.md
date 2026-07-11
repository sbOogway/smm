# sbOogway's market making arbitrage

## about
framework for market making and arbitrage on various exchanges and assets

## architecture
```mermaid
flowchart TD
    subgraph strategy
        disruptor
        subgraph execution [execution]
            direction LR
            subgraph executor [executor]
            end
            subgraph execution_logic [execution_logic]
            end
            
        end
        subgraph common_data_representation
            direction LR
            
            bbo_update
            trade_update
            
            

        end

        subgraph data_provider
            direction LR
            hyperliquid_wss
            polymarket_wss
            betfair_wss
            binance_wss
        end

        subgraph visualization
            direction LR
            mqtt
            grafana
        end

        
    end

    data_provider --> common_data_representation

    common_data_representation --> visualization

    common_data_representation --> disruptor
    disruptor --> execution

    mqtt <--> grafana

    execution_logic --> executor
```

## dependency graph
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