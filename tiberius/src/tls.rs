use std::{io, cmp}; use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::{self, Poll};
use crate::protocol;
use crate::protocol::codec;
use crate::client;
use crate::protocol::codec::Encode;

use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, AsyncReadExt},
    net::TcpStream,
};
use tokio::prelude::*;

#[derive(Debug)]
pub struct TlsTdsWrapper {
    stream: TcpStream,
    bytes_to_write: Vec<u8>,
    bytes_to_read: Vec<u8>,
    bytes_read: usize,
    bytes_written: usize,
    wrap: bool,
    packet_size: usize,
    packet_header: codec::PacketHeader,
}

impl TlsTdsWrapper {
    pub fn new(packet_size: usize, packet_header: codec::PacketHeader, stream: TcpStream) -> Self {
        Self {
            stream,
            bytes_to_write: Vec::new(),
            bytes_to_read: Vec::new(),
            bytes_read: 0,
            bytes_written: 0,
            wrap: true,
            packet_size,
            packet_header,
        }
    }
}

use std::task::Context;

impl AsyncRead for TlsTdsWrapper
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let should_wrap = self.wrap;
        dbg!(&self);
        let mut wtr = self.get_mut();
        let p = Pin::new(&mut wtr.stream);
        if should_wrap {
            // TODO: need to extract the inner TLS message from the PRELOGIN wrapper
            match p.poll_read(cx, buf) {
                Poll::Ready(Ok(br)) => {
                    dbg!(buf);
                    Poll::Ready(Ok(br))
                }
                otherwise => otherwise
            }
        } else {
            p.poll_read(cx, buf) 
        }
    }
}

impl codec::Encode<bytes::BytesMut> for &[u8] {
    fn encode(self, dst: &mut bytes::BytesMut) -> crate::Result<()> {
        dst.copy_from_slice(self);
        Ok(())
    }
}

impl AsyncWrite for TlsTdsWrapper
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
        buf: &[u8]
    ) -> Poll<io::Result<usize>> {
        let should_wrap = self.wrap;
        let mut header = self.packet_header.clone();
        let mut wtr = self.get_mut();
        let p = Pin::new(&mut wtr.stream);
        if should_wrap {
            //let mut payload = bytes::BytesMut::with_capacity(self.packet_size + protocol::HEADER_BYTES);

            let payload = bytes::BytesMut::from(buf);

            header.set_status(codec::PacketStatus::NormalMessage);
            let packet = codec::Packet::new(header, payload);

            let mut write_buf = bytes::BytesMut::new();
            packet.encode(&mut write_buf).unwrap();

            dbg!(&write_buf);
            p.poll_write(cx, &write_buf)
        } else {
            p.poll_write(cx, &buf)
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        dbg!(&self);
        let mut wtr = self.get_mut();
        let p = Pin::new(&mut wtr.stream);
        p.poll_flush(cx)
    }
    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context
    ) -> Poll<io::Result<()>> {
        dbg!(&self);
        let mut wtr = self.get_mut();
        let p = Pin::new(&mut wtr.stream);
        p.poll_shutdown(cx)
    }
}

pub enum MaybeTlsStream {
    Raw(TcpStream),
    #[cfg(feature = "tls")]
    Tls(tokio_tls::TlsStream<TlsTdsWrapper>),
    #[cfg(feature = "rust-tls")]
    RustTls(tokio_rustls::client::TlsStream<TlsTdsWrapper>),
}

impl AsyncRead for MaybeTlsStream {
    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [MaybeUninit<u8>]) -> bool {
        match self {
            MaybeTlsStream::Raw(s) => s.prepare_uninitialized_buffer(buf),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(s) => s.prepare_uninitialized_buffer(buf),
            #[cfg(feature = "rust-tls")]
            MaybeTlsStream::RustTls(s) => s.prepare_uninitialized_buffer(buf),
        }
    }

    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut task::Context,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Raw(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "rust-tls")]
            MaybeTlsStream::RustTls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut task::Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Raw(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "rust-tls")]
            MaybeTlsStream::RustTls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Raw(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "rust-tls")]
            MaybeTlsStream::RustTls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Raw(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "rust-tls")]
            MaybeTlsStream::RustTls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}
