use crate::{
    error::{IOError, KvError},
    pb::abi::{CommandRequest, CommandResponse},
};
use bytes::{Buf, BufMut, BytesMut};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use prost::Message;
use std::io::{Read, Write};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::debug;

// 帧头4字节，Length Prefix Message
pub const Len_Len: usize = 4;
// 1bit压缩位
const COMPRESSIONS_BIT: usize = 1 << 31;
// 长度31bit，所以最大能支持2G大小的frame
// const MAX_FRAME: usize = 2 * 1024 * 1024 * 1024;
const MAX_FRAME: usize = 2 << 31 >> 1;
// 如果payload达到1436字节就做压缩; MTU:1500 - Len_Len:4 - TCP:20 - IP:20 - reserved:20
const COMPRESSION_LIMIT: usize = 1436;

/// 实际应用产品开发的时候，可以直接使用`tokio_util::codec::LengthDelimitedCodec`，功能几乎一样的
/// [LengthDelimitedCodec](https://docs.rs/tokio-util/0.6.8/tokio_util/codec/length_delimited/index.html)
pub trait FrameCoder
where
    Self: Sized + Message + Default,
{
    // 将Message封包(encode)成Frame
    fn encode_frame(&self, buf: &mut BytesMut) -> Result<(), KvError> {
        // return Length of message
        let size = self.encoded_len();
        buf.put_u32(size as _);
        // 达到限制，压缩它
        if size >= COMPRESSION_LIMIT {
            // cache area
            let mut msg_cache = Vec::with_capacity(size);
            self.encode(&mut msg_cache)?;

            // clear LPM, later compressed and merge into it
            let payload = buf.split_off(Len_Len);
            buf.clear();

            // process compression for gzip
            let mut encoder = GzEncoder::new(payload.writer(), Compression::default());
            encoder.write_all(&msg_cache).to_error()?;

            // done compression.
            let payload = encoder.finish().to_error()?.into_inner();
            debug!("Encode a frame: size {}({})", size, payload.len());

            // pushed the compression length
            buf.put_u32((payload.len() | COMPRESSIONS_BIT) as _);

            // merge BytesMut into buf
            buf.unsplit(payload);
        } else {
            self.encode(buf)?;
        }

        Ok(())
    }

    // 将一个Frame解包(decode)成Message
    fn decode_frame(buf: &mut BytesMut) -> Result<Self, KvError> {
        // get 4 byte,And get compressed bit from first bit of it
        let header = buf.get_u32() as usize;
        let (len, compressed) = decode_header(header);
        debug!("Got a frame: msg len {}, compressed {}", len, compressed);

        if compressed {
            // uncompressed
            let mut decoder = GzDecoder::new(&buf[..len]);
            let mut msg_buf = Vec::with_capacity(len * 2);
            decoder.read_to_end(&mut msg_buf).to_error()?;
            buf.advance(len);

            Ok(Self::decode(&msg_buf[..msg_buf.len()])?)
        } else {
            let msg = Self::decode(&buf[..len])?;
            buf.advance(len);
            Ok(msg)
        }
    }
}

impl FrameCoder for CommandRequest {}
impl FrameCoder for CommandResponse {}

/// reading complete frame from stream
pub async fn read_fame<S>(stream: &mut S, buf: &mut BytesMut) -> Result<(), KvError>
where
    S: AsyncRead + Unpin + Send,
{
    let header = stream.read_u32().await.to_error()? as usize;
    let (len, _compressed) = decode_header(header);

    // ensure buffer sufficient of capacity
    buf.reserve(Len_Len + len);
    buf.put_u32(header as _);
    unsafe {
        buf.advance_mut(len);
    }
    // read all
    stream.read_exact(&mut buf[Len_Len..]).await.to_error()?;

    Ok(())
}

#[inline]
fn decode_header(header: usize) -> (usize, bool) {
    let compressed = header & COMPRESSIONS_BIT == COMPRESSIONS_BIT;
    let len = header & !COMPRESSIONS_BIT;
    (len, compressed)
}

#[cfg(test)]
mod frame_tests {

    use anyhow::{Ok, Result};
    use bytes::{Bytes, BytesMut};

    use crate::{
        network::frame::COMPRESSION_LIMIT,
        pb::abi::{CommandRequest, CommandResponse, Value},
    };

    use super::FrameCoder;

    #[test]
    fn command_request_encode_decode_should_work() -> Result<()> {
        let mut buf = BytesMut::new();
        let cmd = CommandRequest::new_hset("t3", "key3", "test frame");
        cmd.encode_frame(&mut buf)?;
        let compressed = is_compressed(&buf);
        assert_eq!(compressed, false);

        let cmd1 = CommandRequest::decode_frame(&mut buf)?;
        assert_eq!(cmd1, cmd);

        Ok(())
    }

    #[test]
    fn command_response_encode_decode_should_work() -> Result<()> {
        let mut buf = BytesMut::new();
        let value: Value = Bytes::from(vec![0u8; COMPRESSION_LIMIT + 1]).into();
        let cmd: CommandResponse = value.into();
        cmd.encode_frame(&mut buf)?;
        let compressed = is_compressed(&buf);
        assert_eq!(compressed, true);

        let cmd1 = CommandResponse::decode_frame(&mut buf)?;
        assert_eq!(cmd1, cmd);

        Ok(())
    }

    fn is_compressed(data: &[u8]) -> bool {
        if let &[v] = &data[..1] {
            v >> 7 == 1
        } else {
            false
        }
    }
}

#[cfg(test)]
mod fn_test_of_read_fame {
    use bytes::BytesMut;
    use tokio::io::AsyncRead;

    use crate::{pb::abi::CommandRequest, test_utils::DummyStream};

    use super::{read_fame, FrameCoder};

    #[tokio::test]
    async fn read_frame_should_work() {
        // 模拟simulation a request
        let mut buf = BytesMut::new();
        let cmd = CommandRequest::new_hdel("t1", "k2");
        cmd.encode_frame(&mut buf).unwrap();
        let mut stream = DummyStream { buf };

        // get request frame form stream
        let mut data = BytesMut::new();
        read_fame(&mut stream, &mut data).await.unwrap();

        let cmd1 = CommandRequest::decode_frame(&mut data).unwrap();

        assert_eq!(cmd, cmd1)
    }
}
