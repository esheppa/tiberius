#[cfg(any(feature = "tls", feature = "rustls"))]
use crate::tds::codec::{Decode, Encode, PacketHeader, PacketStatus, PacketType};
#[cfg(any(feature = "tls", feature = "rustls"))]
use crate::tds::HEADER_BYTES;
#[cfg(any(feature = "tls", feature = "rustls"))]
use bytes::BytesMut;
#[cfg(any(feature = "tls", feature = "rustls"))]
use futures::ready;
#[cfg(any(feature = "tls", feature = "rustls"))]
use std::cmp;
use std::{
    io,
    pin::Pin,
    task::{self, Poll},
};
#[cfg(any(feature = "tls", feature = "rustls"))]
use tracing::{event, Level};

#[cfg(all(feature = "tls", feature = "rustls"))]
compile_error!("`tls` and `rustls` are mutually exclusive");

use futures::{AsyncRead, AsyncWrite};

/// A wrapper to handle either TLS or bare connections.
pub(crate) enum MaybeTlsStream<S: AsyncRead + AsyncWrite + Unpin + Send> {
    Raw(S),
    #[cfg(feature = "tls")]
    NativeTls(async_native_tls::TlsStream<TlsPreloginWrapper<S>>),
    #[cfg(feature = "rustls")]
    Rustls(async_tls::client::TlsStream<TlsPreloginWrapper<S>>),
}

#[allow(dead_code)]
impl<S: AsyncRead + AsyncWrite + Unpin + Send> MaybeTlsStream<S> {
    pub fn into_inner(self) -> S {
        match self {
            Self::Raw(s) => s,
            #[cfg(feature = "tls")]
            Self::NativeTls(mut tls) => tls.get_mut().stream.take().unwrap(),
            #[cfg(feature = "rustls")]
            Self::Rustls(mut tls) => tls.get_mut().stream.take().unwrap(),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncRead for MaybeTlsStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Raw(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            MaybeTlsStream::NativeTls(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "rustls")]
            MaybeTlsStream::Rustls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncWrite for MaybeTlsStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Raw(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            MaybeTlsStream::NativeTls(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "rustls")]
            MaybeTlsStream::Rustls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Raw(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            MaybeTlsStream::NativeTls(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "rustls")]
            MaybeTlsStream::Rustls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Raw(s) => Pin::new(s).poll_close(cx),
            #[cfg(feature = "tls")]
            MaybeTlsStream::NativeTls(s) => Pin::new(s).poll_close(cx),
            #[cfg(feature = "rustls")]
            MaybeTlsStream::Rustls(s) => Pin::new(s).poll_close(cx),
        }
    }
}

/// On TLS handshake, the server expects to get and sends back normal TDS
/// packets. To use a common TLS library, we must implement a wrapper for
/// packet handling on this stage.
///
/// What it does is it interferes on handshake for TDS packet handling,
/// and when complete, just passes the calls to the underlying connection.
#[cfg(any(feature = "tls", feature = "rustls"))]
pub(crate) struct TlsPreloginWrapper<S> {
    stream: Option<S>,
    pending_handshake: bool,

    header_buf: [u8; HEADER_BYTES],
    header_pos: usize,
    read_remaining: usize,

    wr_buf: Vec<u8>,
}

#[cfg(any(feature = "tls", feature = "rustls"))]
impl<S> TlsPreloginWrapper<S> {
    pub fn new(stream: S) -> Self {
        TlsPreloginWrapper {
            stream: Some(stream),
            pending_handshake: true,

            header_buf: [0u8; HEADER_BYTES],
            header_pos: 0,
            read_remaining: 0,
            wr_buf: vec![0u8; HEADER_BYTES],
        }
    }

    pub fn handshake_complete(&mut self) {
        self.pending_handshake = false;
    }
}

#[cfg(any(feature = "tls", feature = "rustls"))]
impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncRead for TlsPreloginWrapper<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        dbg!("READ");
        // Normal operation does not need any extra treatment, we handle packets
        // in the codec.
        if !self.pending_handshake {
            return Pin::new(&mut self.stream.as_mut().unwrap()).poll_read(cx, buf);
        }

        let inner = self.get_mut();

        // Read the headers separately and do not send them to the Tls
        // connection handling.
        if !inner.header_buf[inner.header_pos..].is_empty() {
            while !inner.header_buf[inner.header_pos..].is_empty() {
                let poll = Pin::new(&mut inner.stream.as_mut().unwrap())
                    .poll_read(cx, &mut inner.header_buf[inner.header_pos..]);

                let read = match poll {
                    Poll::Ready(Ok(r)) => r,
                    Poll::Ready(Err(e)) => return Poll::Ready(dbg!(Err(e))),
                    Poll::Pending => {
                        return dbg!(Poll::Pending);
                    }
                };

                if read == 0 {
                    return Poll::Ready(Ok(0));
                }

                inner.header_pos += read;
            }

            let header = PacketHeader::decode(&mut BytesMut::from(&inner.header_buf[..]))
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

            // We only get pre-login packets in the handshake process.
            assert_eq!(header.ty, PacketType::PreLogin);

            // And we know from this point on how much data we should expect
            inner.read_remaining = header.length as usize - HEADER_BYTES;

            event!(
                Level::TRACE,
                "Reading packet of {} bytes",
                inner.read_remaining,
            );
        }

        let max_read = cmp::min(inner.read_remaining, buf.len());

        // TLS connector gets whatever we have after the header.
        let read = ready!(
            Pin::new(&mut inner.stream.as_mut().unwrap()).poll_read(cx, &mut buf[..max_read])
        )?;

        inner.read_remaining -= read;

        // All data is read, after this we're expecting a new header.
        if inner.read_remaining == 0 {
            inner.header_pos = 0;
        }
        dbg!(&read);

        Poll::Ready(Ok(read))
    }
}

#[cfg(any(feature = "tls", feature = "rustls"))]
impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncWrite for TlsPreloginWrapper<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        dbg!("WRITE");
        // Normal operation does not need any extra treatment, we handle
        // packets in the codec.
        if !self.pending_handshake {
            return Pin::new(&mut self.stream.as_mut().unwrap()).poll_write(cx, buf);
        } 


        self.wr_buf.extend_from_slice(buf);

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        dbg!("FLUSH");
        let inner = self.get_mut();

        // If on handshake mode, wraps the data to a TDS packet before sending.
        if inner.pending_handshake {
            let mut header = PacketHeader::new(inner.wr_buf.len(), 0);

            header.ty = PacketType::PreLogin;
            header.status = PacketStatus::EndOfMessage;

            header
                .encode(&mut &mut inner.wr_buf[0..HEADER_BYTES])
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "Could not encode header.")
                })?;


            while !inner.wr_buf.is_empty() {
                event!(
                    Level::TRACE,
                    "Writing a packet of {} bytes",
                    inner.wr_buf.len(),
                );

                let written =
                    ready!(Pin::new(&mut inner.stream.as_mut().unwrap())
                        .poll_write(cx, &mut inner.wr_buf))?;

                inner.wr_buf.drain(..written);
            }

            inner.wr_buf.resize(HEADER_BYTES, 0);
        }

        Pin::new(&mut inner.stream.as_mut().unwrap()).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream.as_mut().unwrap()).poll_close(cx)
    }
}
