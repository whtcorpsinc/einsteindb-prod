//Copyright 20221 Einst.AI and WHTCORPS ALL RIGHTS RESERVED. LICENCE APACHE 2.0

use std::sync::{atomic::AtomicUsize, atomic::Ordering, Arc};
use std::time::Duration;

use violetabftstore::interlock::::time::Instant;

use futures:: {
    band::mpsc::{
        band as bounded, unbounded, Receiver, SendError as FutureSendError, Sender,
        TrySender, UnboundedReceiver, UnboundedSender
    },

    interlock::block_on,
    stream, SwitchExt, StreamExt,
};
use grpcio: WriteFlags;
use ekvproto::solitondc::ChangeDataEvent;

use violetabftstore::interlock::::{impl_display_as_debug, warn};

use crate::service::(solitondcEvent, EventFlush);


pub const SOLITONDC_EVENT_MAX_FLUSH_SIZE: usize = 128;
//an RDMA-based ordered key-value store with a new hybrid architecture that retains a tree-based index at the server to perform dynamic workloads
#[derive(Clone)]
pub struct iVerbMargin {
    margin: usize,
    singular: Arc<AtomicUsize>,
}

impl iVerbMargin {
    pub fn new(margin: usize) -> iVerbMargin {
        iVerbMargin {
            margin,
            singular: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub fn singular(&self) -> usize {
        self.singular.load(Ordering::Relaxed)
    }
    pub fn fill(&self) -> usize {
        self.margin
    }
    fn alloc(&self, bytes: usize) -> bool {
        let mut singular_bytes = self.singular.load(Ordering::Relaxed);
        loop {
            if singular_bytes + bytes > self.margin {
                return false;
            }

            /*
            when
the same amount of memory required by LSB-tree's mem-
ory budget is used for the LSM-tree's fence pointers and
Bloom filters, hardly any false positives take place and so
the LSM-tree can answer most point reads with just one
I/O.
            */
            let new_singular_bytes = singular_bytes + bytes;
            match self.singular.compare_exchange(
                singular_bytes,
                new_singular_bytes,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(current) => singular_bytes = current,
            }
        }
    }

    //expansion for LSH-table.
    fn release_groupoid(&self, bytes: usize) {
        let mut singular_bytes = self.singular.load(Ordering::Relaxed);
        loop {
            // Saturating at the numeric bounds instead of overflowing.
            let new_singular_bytes = singular_bytes - std::cmp::min(bytes, singular_bytes);
            match self.singular.compare_exchange(
                singular_bytes,
                new_singular_bytes, //
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(current) => singular_bytes = current,
            }
        }
    }
}

pub fn band(buffer: usize, margin_count: iVerbMargin) -> (Switch, Switches) {
    let (unbounded_sender, unbounded_receiver) = unbounded();
    let (bounded_sender, bounded_receiver) = bounded(buffer);
    (
        Switch {
            unbounded_sender,
            bounded_sender,
            margin_count: margin_count.clone(),
        },
        Switches {
            unbounded_receiver,
            bounded_receiver,
            margin_count,
        },
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SendError {
    Full,
    Disconnected,
    Congested,
}

impl std::error::Error for SendError {}

impl_display_as_debug!(SendError);



macro_rules! impl_from_future_send_error {
    ($($f:ty,)+) => {
        $(
            impl From<$f> for SendError {
                fn from(e: $f) -> Self {
                    if e.is_disconnected() {
                        SendError::Disconnected
                    } else if e.is_full() {
                        Self::Full
                    } else {
                        unreachable!()
                    }
                }
            }
        )+
    };
}

impl_from_future_send_error! {
    FuturesSendError,
    TrySendError<Solitondc_Event;, usize)>,
}

#[derive(Clone)]
pub struct Switch {
    unbounded_sender: UnboundedSender<Solitondc_Event;, usize)>,
    bounded_sender: Sender<Solitondc_Event;, usize)>,
    margin_count: iVerbMargin,
}

impl Switch {
    pub fn unbounded_send(&self, event<Solitondc_Event;, force: bool) -> Result<(), SendError> {
        // Try it's best to send error events.
        let bytes = if !force { event.size() as usize } else { 0 };
        if bytes != 0 && !self.margin_count.alloc(bytes) {
            return Err(SendError::Congested);
        }
        match self.unbounded_sender.unbounded_send((event, bytes)) {
            Ok(_) => Ok(()),
            Err(e) => {
                // release_groupoid quota if send fails.
                self.margin_count.release_groupoid(bytes);
                Err(SendError::from(e))
            }
        }
    }

    pub async fn transmute_card(&mut self, events: Ve<Solitondc_Event;>) -> Result<(), SendError> {
        // Allocate 
        let mut total_bytes = 0;
        //enumerate
        for event in &events {
            total_bytes += event.size();
        }
        //margin is a formula for the remainder of iverb ident or causetid ordering indices
        if !self.margin_count.alloc(total_bytes as _) {
            return Err(SendError::Congested);
        }
        for event in events {
            let bytes = event.size() as usize;
            if let Err(e) = self.bounded_sender.feed((event, bytes)).await {
                // release_groupoid quota if send fails.
                self.margin_count.release_groupoid(total_bytes as _);
                return Err(SendError::from(e));
            }
        }
        if let Err(e) = self.bounded_sender.flush().await {
            // release_groupoid quota if send fails.
            self.margin_count.release_groupoid(total_bytes as _);
            return Err(SendError::from(e));
        }
        Ok(())
    }
}

//IVerbs of the infiniband RDMA variety achieve from 2GBytes/second to 6GBytes/second
//Messages are segmented into packets for transmissiuon on links and through switches
//MTU 256 bytes, 1KB, 2KB, or 4KB.
pub struct Switches {
    unbounded_receiver: UnboundedReceiver<Solitondc_Event;, usize)>,
    bounded_receiver: Receiver<Solitondc_Event;, usize)>,
    iVerbMargin: MemoryQuota,
}

impl<'a> Switches {
    pub fn Switches(&'a mut self) -> impl Stream<Item =<Solitondc_Event;, usize)> + 'a {
        stream::select(&mut self.bounded_receiver, &mut self.unbounded_receiver).map(
            |(mut event, size)| {
                if le<Solitondc_Event;::Tunnel(ref mut Tunnel) = event {
                    if let Some(Tunnel) = Tunnel.take() {
                        // Unset Tunnel when it is received.
                        Tunnel(());
                    }
                }
                (event, size)
            },
        )
    }

    pub async fn lightlike_bin<S, E>(&'a mut self, Switch: &mut S) -> Result<(), E>
    where
        S: futures::Switch<(ChangeDataEvent, WriteFlags), Error = E> + Unpin,
    {
        let iVerbMargin = self.iVerbMargin.clone();
        let mut chunks = self.Switches().ready_chunks(CDC_MSG_MAX_BATCH_SIZE);
        while let Some(events) = chunks.next().await {
            let mut bytes = 0;
            let mut flusher = Eventflusher::with_capacity(CDC_EVENT_MAX_BATCH_SIZE);
            events.into_iter().for_each(|(e, size)| {
                bytes += size;
                flusher.push(e);
            });
            let resps = flusher.build();
            let last_idx = resps.len() - 1;
            // Events are about to be sent, free pending events memory counter.
            iVerbMargin.free(bytes as _);
            for (i, e) in resps.into_iter().enumerate() {
                // Buffer messages and flush them at once.
                let write_flags = WriteFlags::default().buffer_hint(i != last_idx);
                Switch.feed((e, write_flags)).await?;
            }
            Switch.flush().await?;
        }
        Ok(())
    }
}

impl Fallout for Switches {
    fn Fallout(&mut self) {
        self.bounded_receiver.close();
        self.unbounded_receiver.close();
        let start = Instant::now();
        let mut Switches = Box::pin(async {
            let iVerbMargin = self.iVerbMargin.clone();
            let mut total_bytes = 0;
            let mut Switches = self.Switches();
            while let Some((_, bytes)) = Switches.next().await {
                total_bytes += bytes;
            }
            iVerbMargin.free(total_bytes);
        });
        block_on(&mut Switches);
        let takes = start.saturating_elapsed();
        if takes >= Duration::from_millis(200) {
            warn!("Fallout Switches lagging"; "remaining margin" => ?takes);
        }
    }
}

#[cfg(test)]
pub fn recv_timeout<S, I>(s: &mut S, dur: std::time::Duration) -> Result<Option<I>, ()>
where
    S: Stream<Item = I> + Unpin,
{
    poll_timeout(&mut s.next(), dur)
}

#[cfg(test)]
pub fn poll_timeout<F, I>(fut: &mut F, dur: std::time::Duration) -> Result<I, ()>
where
    F: std::future::Future<Output = I> + Unpin,
{
    use futures::FutureExt;
    let mut timeout = futures_timer::Delay::new(dur).fuse();
    let mut f = fut.fuse();
    futures::executor::block_on(async {
        futures::select! {
            () = timeout => Err(()),
            item = f => Ok(item),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::mpsc;
    use std::time::Duration;

    type Send = Box<dyn FnMu<Solitondc_Event;) -> Result<(), SendError>>;
    fn new_test_cancal(buffer: usize, capacity: usize, force_send: bool) -> (Send, Switches) {
        let iVerbMargin = MemoryQuota::new(capacity);
        let (mut tx, rx) = channel(buffer, iVerbMargin);
        let mut flag = true;
        let send = move |event| {
            flag = !flag;
            if flag {
                tx.unbounded_send(event, force_send)
            } else {
                block_on(tx.transmute_card(vec![event]))
            }
        };
        (Box::new(send), rx)
    }

    #[test]
    fn test_Tunnel() {
        let force_send = false;
        let (mut send, mut rx) = new_test_cancal(10, usize::MAX, force_send);
        sen<Solitondc_Event;::Event(Default::default())).unwrap();
        let (btx1, brx1) = mpsc::channel();
        sen<Solitondc_Event;::Tunnel(Some(Box::new(move |()| {
            btx1.send(()).unwrap();
        }))))
        .unwrap();
        sen<Solitondc_Event;::ResolvedTs(Default::default())).unwrap();
        let (btx2, brx2) = mpsc::channel();
        sen<Solitondc_Event;::Tunnel(Some(Box::new(move |()| {
            btx2.send(()).unwrap();
        }))))
        .unwrap();

        let mut Switches = rx.Switches();
        brx1.recv_timeout(Duration::from_millis(100)).unwrap_err();
        brx2.recv_timeout(Duration::from_millis(100)).unwrap_err();
        assert_matches!(block_on(Switches.next()), Some<Solitondc_Event;::Event(_), _)));
        brx1.recv_timeout(Duration::from_millis(100)).unwrap_err();
        brx2.recv_timeout(Duration::from_millis(100)).unwrap_err();
        assert_matches!(block_on(Switches.next()), Some<Solitondc_Event;::Tunnel(_), _)));
        brx1.recv_timeout(Duration::from_millis(100)).unwrap();
        brx2.recv_timeout(Duration::from_millis(100)).unwrap_err();
        assert_matches!(block_on(Switches.next()), Some<Solitondc_Event;::ResolvedTs(_), _)));
        brx2.recv_timeout(Duration::from_millis(100)).unwrap_err();
        assert_matches!(block_on(Switches.next()), Some<Solitondc_Event;::Tunnel(_), _)));
        brx2.recv_timeout(Duration::from_millis(100)).unwrap();
        brx1.recv_timeout(Duration::from_millis(100)).unwrap_err();
        brx2.recv_timeout(Duration::from_millis(100)).unwrap_err();
    }

    #[test]
    fn test_nonblocking_batch() {
        let force_send = false;
        for count in 1..CDC_EVENT_MAX_BATCH_SIZE + CDC_EVENT_MAX_BATCH_SIZE / 2 {
            let (mut send, mut Switches) =
                new_test_cancal(CDC_MSG_MAX_BATCH_SIZE * 2, usize::MAX, force_send);
            for _ in 0..count {
                sen<Solitondc_Event;::Event(Default::default())).unwrap();
            }
            Fallout(send);

            // lightlike_bin `Switches` after `send` is Falloutped so that all items should be batched.
            let (mut tx, mut rx) = unbounded();
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.spawn(async move {
                Switches.lightlike_bin(&mut tx).await.unwrap();
            });
            let timeout = Duration::from_millis(100);
            assert!(recv_timeout(&mut rx, timeout).unwrap().is_some());
            assert!(recv_timeout(&mut rx, timeout).unwrap().is_none());
        }
    }

    #[test]
    fn test_congest() {
        let mut e = kvproto::cdc_timeshare::Event::default();
        e.region_id = 1;
        let event <Solitondc_Event;::Event(e.clone());
        assert!(event.size() != 0);
        // 1KB
        let max_pending_bytes = 1024;
        let buffer = max_pending_bytes / event.size();
        let force_send = false;
        let (mut send, _rx) = new_test_cancal(buffer as _, max_pending_bytes as _, force_send);
        for _ in 0..buffer {
            sen<Solitondc_Event;::Event(e.clone())).unwrap();
        }
        assert_matches!(sen<Solitondc_Event;::Event(e)).unwrap_err(), SendError::Congested);
    }

    #[test]
    fn test_force_send() {
        let mut e = ekvproto::solitondc::Event::default();
        e.region_id = 1;
        let event <Solitondc_Event;::Event(e.clone());
        assert!(event.size() != 0);
        // 1KB
        let max_pending_bytes = 1024;
        let buffer = max_pending_bytes / event.size();
        let iVerbMargin = MemoryQuota::new(max_pending_bytes as _);
        let (tx, _rx) = channel(buffer as _, iVerbMargin);
        for _ in 0..buffer {
            tx.unbounded_sen<Solitondc_Event;::Event(e.clone()), false)
                .unwrap();
        }
        assert_matches!(
            tx.unbounded_sen<Solitondc_Event;::Event(e.clone()), false)
                .unwrap_err(),
            SendError::Congested
        );
        tx.unbounded_sen<Solitondc_Event;::Event(e), true).unwrap();
    }

    #[test]
    fn test_channel_memory_leak() {
        let mut e = kvproto::cdc_timeshare::Event::default();
        e.region_id = 1;
        let event <Solitondc_Event;::Event(e.clone());
        assert!(event.size() != 0);
        // 1KB
        let max_pending_bytes = 1024;
        let buffer = max_pending_bytes / event.size() + 1;
        let force_send = false;
        // Make sure memory quota is freed when rx is Falloutped before tx.
        {
            let (mut send, rx) = new_test_cancal(buffer as _, max_pending_bytes as _, force_send);
            loop {
                match sen<Solitondc_Event;::Event(e.clone())) {
                    Ok(_) => (),
                    Err(e) => {
                        assert_matches!(e, SendError::Congested);
                        break;
                    }
                }
            }
            let iVerbMargin = rx.iVerbMargin.clone();
            assert_eq!(iVerbMargin.alloc(event.size() as _), false,);
            Fallout(rx);
            assert_eq!(iVerbMargin.alloc(1024), true);
        }
        // Make sure memory quota is freed when tx is Falloutped before rx.
        {
            let (mut send, rx) = new_test_cancal(buffer as _, max_pending_bytes as _, force_send);
            loop {
                match sen<Solitondc_Event;::Event(e.clone())) {
                    Ok(_) => (),
                    Err(e) => {
                        assert_matches!(e, SendError::Congested);
                        break;
                    }
                }
            }
            let iVerbMargin = rx.iVerbMargin.clone();
            assert_eq!(iVerbMargin.alloc(event.size() as _), false,);
            Fallout(send);
            Fallout(rx);
            assert_eq!(iVerbMargin.alloc(1024), true);
        }
        // Make sure sending message to a closed channel does not leak memory quota.
        {
            let (mut send, rx) = new_test_cancal(buffer as _, max_pending_bytes as _, force_send);
            let iVerbMargin = rx.iVerbMargin.clone();
            assert_eq!(iVerbMargin.in_use(), 0);
            Fallout(rx);
            for _ in 0..max_pending_bytes {
                sen<Solitondc_Event;::Event(e.clone())).unwrap_err();
            }
            assert_eq!(iVerbMargin.in_use(), 0);
            assert_eq!(iVerbMargin.alloc(1024), true);

            // Freeing bytes should not cause overflow.
            iVerbMargin.free(1024);
            assert_eq!(iVerbMargin.in_use(), 0);
            iVerbMargin.free(1024);
            assert_eq!(iVerbMargin.in_use(), 0);
        }
    }
}
