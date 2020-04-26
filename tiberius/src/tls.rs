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

//impl io::Read for PreloginWrappedTcpStream {
//    #[inline]
//    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
//        // read a new packet header, when required
//        if self.bytes_left == 0 {
//            self.flush()?;
//            let mut header_bytes = [0u8; protocol::HEADER_BYTES];
//            let end_pos = header_bytes.len() - self.bytes_to_read.len();
//            let amount = self.stream.read(&mut header_bytes[..end_pos])?;
//
//            self.bytes_to_read.extend_from_slice(&header_bytes[..amount]);
//            if self.bytes_to_read.len() == protocol::HEADER_BYTES {
//                let header = codec::PacketHeader::unserialize(&self.bytes_to_read).map_err(|_| {
//                    io::Error::new(io::ErrorKind::InvalidInput, "malformed packet header")
//                })?;
//                self.bytes_left = header.length as usize - protocol::HEADER_BYTES;
//                self.bytes_to_read.truncate(0);
//            }
//        }
//
//        // read as much data as required
//        if self.bytes_left > 0 {
//            let end_pos = cmp::min(self.bytes_left, buf.len());
//            let amount = self.stream.read(&mut buf[..end_pos])?;
//            if amount == 0 {
//                return Ok(0);
//            }
//            self.bytes_left -= amount;
//            return Ok(amount);
//        }
//
//        Ok(0)
//    }

use std::task::Context;

impl AsyncRead for TlsTdsWrapper
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        // here we need to remove the framing
        dbg!(&self);
        let mut wtr = self.get_mut();
        let p = Pin::new(&mut wtr.stream);
        match p.poll_read(cx, buf) {
            Poll::Ready(Ok(br)) => {
                dbg!(br);
                dbg!(buf);
                Poll::Ready(Ok(br))
            }
            otherwise => otherwise
        }
        //self.stream.poll_read(cx, buf)
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
        // here we need to add in the proper framing
        //let mut payload = bytes::BytesMut::with_capacity(self.packet_size + protocol::HEADER_BYTES);
        let mut header = self.packet_header.clone();

        let payload = bytes::BytesMut::from(buf);
        //if let Err(e) = buf.encode(&mut payload) {
            //return Poll::Ready(Err(e));
         //   return Poll::Ready(Err(io::ErrorKind::InvalidData.into()));
        //}

        dbg!("ENCODED BUFF");
        header.set_status(codec::PacketStatus::NormalMessage);
        let packet = codec::Packet::new(header, payload);

        let mut write_buf = bytes::BytesMut::new();
        packet.encode(&mut write_buf).unwrap();

        let mut wtr = self.get_mut();
        let p = Pin::new(&mut wtr.stream);
        dbg!(&write_buf);
        p.poll_write(cx, &write_buf)

//        dbg!(&buf);
//        // the full amount to write
//        //self.bytes_to_write.extend_from_slice(buf);
//        //Poll::Ready(Ok(buf.len()))
//
//        if !self.bytes_to_write.is_empty() {
//            //while !payload.is_empty() {
//                let writable = cmp::min(payload.len(), self.packet_size);
//
//                let split_payload = bytes::BytesMut::from(&payload[..writable]);
//                payload = bytes::BytesMut::from(&payload[writable..]);
//
//                if payload.is_empty() {
//                    header.set_status(codec::PacketStatus::EndOfMessage);
//                } else {
//                    header.set_status(codec::PacketStatus::NormalMessage);
//                }
//
//    //            event!(
//    //                Level::TRACE,
//    //                "Encoding a packet from a buffer of size {}",
//    //                split_payload.len() + HEADER_BYTES,
//    //            );
//
//                let packet = codec::Packet::new(header, split_payload);
//                // need to reutrn this properlty
//                let mut write_buf = bytes::BytesMut::new();
//                packet.encode(&mut write_buf).unwrap();
//                let mut wtr = self.get_mut();
//                let p = Pin::new(&mut wtr.stream);
//                dbg!(&write_buf);
//                p.poll_write(cx, &write_buf);
//                //self.transport.send(packet).await?;
//            //}
//
//            Poll::Pending
//        } else {
//            Poll::Ready(Ok(self.bytes_written))
//        }
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
