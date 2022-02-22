use std::sync::Arc;

use arc_swap::ArcSwap;

#[derive(Clone, Debug)]
pub struct Socket {
    inner: Arc<Inner>,
}

#[derive(Debug)]
pub struct Inner {
    filters: crate::filters::SharedFilterChain,
    socket: ArcSwap<std::net::UdpSocket>,
}

impl Socket {
    pub async fn bind<A: std::net::ToSocketAddrs>(addrs: A, filters: &[crate::config::Filter]) -> crate::Result<Self> {
        let socket = ArcSwap::new(Arc::new(std::net::UdpSocket::bind(addrs)?));

        Ok(Self {
            inner: Arc::new(Inner {
                filters,
                socket,
            })
        })
    }

    pub async fn recv_from(&self) -> Result<Option<Vec<u8>>> {
        let mut buf = Vec::new();
        let (length, addr) = self.socket.recv_from(&mut buf).await?;
        let result = self.filter_chain.read(ReadContext::new(
            self.endpoints.load(),
            addr,
            Vec::from(buf[..length]),
        ));

        result.map(|res| res.contents)
    }
}

pub struct Builder {
    filters: Vec<()>,
}
