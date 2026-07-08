use std::time::Duration;

use disruptor::{
    MultiProducer, ProcessorSettings, SingleConsumerBarrier, Sleep,
};


pub struct Disruptor<T> {
    pub producer: MultiProducer<T, SingleConsumerBarrier>,
}

impl<T> Disruptor<T>
where
    T: Send + Sync + 'static,
{
    pub fn new<F, G>(size: usize, factory: F, mut processor: G) -> Self
    where
        G: FnMut(&T, i64, bool) + Send + 'static,
        F: Fn() -> T + Send + Sync + 'static,
    {
        // for production use BusySpin
        let wait_strategy = Sleep::new(Duration::from_millis(1));
        let producer = disruptor::build_multi_producer(size, factory, wait_strategy)
            .pin_at_core(1)
            .handle_events_with(move |event, seq, batch| {
                processor(event, seq, batch);
            })
            .build();

        Self { producer }
    }
}