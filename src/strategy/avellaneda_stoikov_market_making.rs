use disruptor::{MultiProducerBarrier, SingleConsumerBarrier};

use crate::strategy::{common_data_representation::{disruptor::Disruptor, price_update::PriceUpdate}, exchange::traits::DataProvider};


pub struct AvellanedaStoikovMarketMaking {
    // data_provider: &'static dyn DataProvider,
    disruptor: &'static MultiProducerBarrier,

    
}

impl AvellanedaStoikovMarketMaking {

}