use std::sync::{Arc, Weak};

use bbqueue::{
    framed::{FrameConsumer, FrameGrantR, FrameGrantW, FrameProducer},
    BBBuffer,
};

pub fn channel<const N: usize>(
    bbs: &[BBBuffer<N>],
) -> bbqueue::Result<(Vec<Producer<N>>, Consumer<N>)> {
    let refcount = Arc::new(());
    let mut producers = Vec::with_capacity(bbs.len());
    let mut consumers = Vec::with_capacity(bbs.len());

    for bb in bbs {
        let (producer, consumer) = bb.try_split_framed()?;
        producers.push(Producer {
            producer,
            _refcount: refcount.clone(),
        });
        consumers.push(consumer);
    }

    Ok((
        producers,
        Consumer {
            consumers,
            refcount: Arc::downgrade(&refcount),
        },
    ))
}

pub struct Consumer<'a, const N: usize> {
    consumers: Vec<FrameConsumer<'a, N>>,
    refcount: Weak<()>,
}

impl<'a, const N: usize> Consumer<'a, N> {
    pub fn read(&mut self) -> Option<FrameGrantR<'a, N>> {
        loop {
            match self.consumers.iter_mut().find_map(|c| c.read()) {
                Some(frame) => return Some(frame),
                None if self.refcount.strong_count() == 0 => return None,
                None => (),
            }
        }
    }
}

pub struct Producer<'a, const N: usize> {
    producer: FrameProducer<'a, N>,
    // TODO Replace this by something simpler?
    // TODO How can we handle a consumer hangup? (reverse Arc, crossbeam channel)
    _refcount: Arc<()>,
}

impl<'a, const N: usize> Producer<'a, N> {
    pub fn grant(&mut self, max_sz: usize) -> bbqueue::Result<FrameGrantW<'a, N>> {
        self.producer.grant(max_sz)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let bbs: Vec<_> = std::iter::repeat_with(BBBuffer::<1000>::new)
            .take(100)
            .collect();

        let (producers, mut consumer) = super::channel(&bbs).unwrap();

        std::thread::scope(|s| {
            for (pid, mut producer) in producers.into_iter().enumerate() {
                s.spawn(move || {
                    for _ in 0..50 {
                        const SIZE: usize = 100;
                        let mut frame = loop {
                            match producer.grant(SIZE) {
                                Ok(frame) => break frame,
                                Err(bbqueue::Error::InsufficientSize) => continue,
                                Err(e) => panic!("{e:?}"),
                            }
                        };
                        frame.iter_mut().for_each(|b| *b = pid as u8);
                        frame.commit(SIZE);
                    }
                });
            }

            while let Some(frame) = consumer.read() {
                let (first, remaining) = frame.split_first().unwrap();
                assert!(remaining.iter().all(|b| b == first));
                frame.release();
            }
        })
    }
}
