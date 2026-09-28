use core::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::sync::oneshot::Receiver;
use tokio_stream::Stream;

// adapted from https://github.com/tokio-rs/tokio/blob/master/tokio-stream/src/wrappers/mpsc_bounded.rs

/// A wrapper around [`tokio::sync::oneshot::Receiver`] that implements [`Stream`].
/// Allows a oneshot receiver to be polled along side other streams
#[derive(Debug)]
pub struct ReceiverStream<T> {
    inner: Receiver<T>,
}

impl<T> ReceiverStream<T> {
    pub fn new(recv: Receiver<T>) -> Self {
        Self { inner: recv }
    }

    pub fn into_inner(self) -> Receiver<T> {
        self.inner
    }

    pub fn close(&mut self) {
        self.inner.close();
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

impl<T> Stream for ReceiverStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_terminated() {
            return Poll::Ready(None);
        }

        let inner_pin = Pin::new(&mut self.inner);
        match inner_pin.poll(cx) {
            Poll::Ready(val) => {
                match val {
                    Ok(item) => Poll::Ready(Some(item)),
                    Err(_) => Poll::Ready(None), //if there is an error no value will ever be produced
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> AsRef<Receiver<T>> for ReceiverStream<T> {
    fn as_ref(&self) -> &Receiver<T> {
        &self.inner
    }
}

impl<T> AsMut<Receiver<T>> for ReceiverStream<T> {
    fn as_mut(&mut self) -> &mut Receiver<T> {
        &mut self.inner
    }
}

impl<T> From<Receiver<T>> for ReceiverStream<T> {
    fn from(recv: Receiver<T>) -> Self {
        Self::new(recv)
    }
}
