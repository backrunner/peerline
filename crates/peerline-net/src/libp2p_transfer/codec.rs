use crate::protocol::{MAX_FRAME, WireFrame, decode_wire, encode_wire};
use futures::{AsyncReadExt, AsyncWriteExt};
use libp2p::{StreamProtocol, request_response::Codec};

#[derive(Clone, Debug, Default)]
pub(crate) struct WireCodec;

#[async_trait::async_trait]
impl Codec for WireCodec {
    type Protocol = StreamProtocol;
    type Request = WireFrame;
    type Response = WireFrame;

    async fn read_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Request>
    where
        T: futures::AsyncRead + Unpin + Send,
    {
        read_framed(io).await
    }

    async fn read_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Response>
    where
        T: futures::AsyncRead + Unpin + Send,
    {
        read_framed(io).await
    }

    async fn write_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> std::io::Result<()>
    where
        T: futures::AsyncWrite + Unpin + Send,
    {
        write_framed(io, &req).await
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> std::io::Result<()>
    where
        T: futures::AsyncWrite + Unpin + Send,
    {
        write_framed(io, &res).await
    }
}

async fn write_framed<W: futures::AsyncWrite + Unpin>(
    io: &mut W,
    frame: &WireFrame,
) -> std::io::Result<()> {
    let payload = encode_wire(frame)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    io.write_all(&(payload.len() as u32).to_be_bytes()).await?;
    io.write_all(&payload).await?;
    Ok(())
}

async fn read_framed<R: futures::AsyncRead + Unpin>(io: &mut R) -> std::io::Result<WireFrame> {
    let mut len = [0u8; 4];
    io.read_exact(&mut len).await?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut payload = vec![0u8; len];
    io.read_exact(&mut payload).await?;
    decode_wire(&payload).map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
}
