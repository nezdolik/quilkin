/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

use std::{
    convert::{TryFrom, TryInto},
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs},
    str::FromStr,
};

use serde::{Deserialize, Serialize};

use crate::xds::envoy::config::core::v3::{
    address::Address as EnvoyAddress, SocketAddress as EnvoySocketAddress,
};

/// A valid socket address. This differs from `std::net::SocketAddr`, in that it
/// it supports parsing Domain Names in addition to IP addresses. Domain Names
/// are resolved when `ToSocketAddrs::to_socket_addrs` is called.
#[derive(Debug, PartialEq, Clone, PartialOrd, Ord, Eq, Hash, Serialize)]
pub struct EndpointAddress {
    /// A valid name or IP address that resolves to a address.
    pub host: AddressKind,
    /// The port of the socket address, if present.
    pub port: Option<u16>,
}

impl EndpointAddress {
    pub const UNSPECIFIED: Self = Self {
        host: AddressKind::Ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
        port: Some(0),
    };
    pub const LOCALHOST: Self = Self {
        host: AddressKind::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        port: Some(0),
    };
}

impl EndpointAddress {
    /// Returns the port for the endpoint address, or `0` if no port
    /// was specified.
    pub fn port(&self) -> u16 {
        self.port.unwrap_or(0)
    }

    pub(crate) async fn lookup_host<A: tokio::net::ToSocketAddrs>(addr: A) -> crate::Result<Self> {
        Ok(Self::from(
            tokio::net::lookup_host(addr)
                .await?
                .next()
                .ok_or_else(|| eyre::eyre!("No valid socket found."))?,
        ))
    }

    /// Returns the socket address for the endpoint, resolving any DNS entries
    /// if present.
    pub fn to_socket_addr(&self) -> crate::Result<SocketAddr> {
        let no_valid_socket_err = || eyre::eyre!("No valid socket address found.");
        if let Some(port) = self.port {
            match &self.host {
                AddressKind::Ip(ip) => (*ip, port)
                    .to_socket_addrs()?
                    .next()
                    .ok_or_else(no_valid_socket_err),
                AddressKind::Name(name) => (&**name, port)
                    .to_socket_addrs()?
                    .next()
                    .ok_or_else(no_valid_socket_err),
            }
        } else {
            match &self.host {
                AddressKind::Ip(_) => Err(eyre::eyre!("No port provided for address.")),
                AddressKind::Name(name) => name
                    .to_socket_addrs()?
                    .next()
                    .ok_or_else(no_valid_socket_err),
            }
        }
    }
}

/// Forwards the deserialisation to use [`std::net::ToSocketAddrs`] instead of
/// [`FromStr`] for validation which allows us to resolve DNS hostnames such as
/// `localhost` or container network names at parse-time.
impl FromStr for EndpointAddress {
    type Err = eyre::Report;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        string
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| eyre::eyre!("No valid socket address found."))?;

        Ok(match string.split_once(':') {
            Some((host, port)) => {
                let host = host.parse().unwrap();
                let port = port.parse()?;

                Self {
                    host,
                    port: Some(port),
                }
            }
            _ => Self {
                host: string.parse().unwrap(),
                port: None,
            },
        })
    }
}

impl From<SocketAddr> for EndpointAddress {
    fn from(socket: SocketAddr) -> Self {
        Self {
            host: AddressKind::Ip(socket.ip()),
            port: Some(socket.port()),
        }
    }
}

impl From<(IpAddr, u16)> for EndpointAddress {
    fn from((ip, port): (IpAddr, u16)) -> Self {
        Self {
            host: AddressKind::Ip(ip),
            port: Some(port),
        }
    }
}

impl From<(Ipv4Addr, u16)> for EndpointAddress {
    fn from((ip, port): (Ipv4Addr, u16)) -> Self {
        Self {
            host: AddressKind::Ip(IpAddr::V4(ip)),
            port: Some(port),
        }
    }
}

impl From<([u8; 4], u16)> for EndpointAddress {
    fn from((ip, port): ([u8; 4], u16)) -> Self {
        Self {
            host: AddressKind::Ip(IpAddr::V4(ip.into())),
            port: Some(port),
        }
    }
}

impl From<(Ipv6Addr, u16)> for EndpointAddress {
    fn from((ip, port): (Ipv6Addr, u16)) -> Self {
        Self {
            host: AddressKind::Ip(IpAddr::V6(ip)),
            port: Some(port),
        }
    }
}

impl TryFrom<EnvoySocketAddress> for EndpointAddress {
    type Error = eyre::Error;

    fn try_from(value: EnvoySocketAddress) -> Result<Self, Self::Error> {
        use crate::xds::envoy::config::core::v3::socket_address::PortSpecifier;

        let address = Self {
            host: value.address.parse()?,
            port: match value.port_specifier {
                Some(PortSpecifier::PortValue(value)) => Some(value.try_into()?),
                Some(PortSpecifier::NamedPort(_)) => {
                    return Err(eyre::eyre!("Named ports are not supported."))
                }
                None => None,
            },
        };

        // Ensure the address from envoy resolves to an address.
        address.to_socket_addrs()?;

        Ok(address)
    }
}

impl TryFrom<EnvoyAddress> for EndpointAddress {
    type Error = eyre::Error;

    fn try_from(value: EnvoyAddress) -> Result<Self, Self::Error> {
        match value {
            EnvoyAddress::SocketAddress(address) => address.try_into(),
            _ => Err(eyre::eyre!("Unsupported Envoy address type.")),
        }
    }
}

impl fmt::Display for EndpointAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.host)?;

        if let Some(port) = self.port {
            write!(f, ":{}", port)?;
        }

        Ok(())
    }
}

impl ToSocketAddrs for EndpointAddress {
    type Iter = <std::net::SocketAddr as std::net::ToSocketAddrs>::Iter;

    fn to_socket_addrs(&self) -> std::io::Result<Self::Iter> {
        self.to_socket_addr()
            .map(Some)
            .map(IntoIterator::into_iter)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::Other, error))
    }
}

impl<'de> Deserialize<'de> for EndpointAddress {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Accept borrowed or owned strings.
        let string = <std::borrow::Cow<'de, str>>::deserialize(de)?;
        string.parse::<Self>().map_err(serde::de::Error::custom)
    }
}

/// The kind of address, such as Domain Name or IP address. **Note** that
/// the `FromStr` implementation doesn't actually validate that the name is
/// resolvable. Use [`EndpointAddress`] for complete address validation.
#[derive(Debug, PartialEq, Clone, PartialOrd, Eq, Hash, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AddressKind {
    Name(String),
    Ip(IpAddr),
}

impl FromStr for AddressKind {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(s.parse()
            .map(Self::Ip)
            .unwrap_or_else(|_| Self::Name(s.to_owned())))
    }
}

impl fmt::Display for AddressKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Name(name) => name.fmt(f),
            Self::Ip(ip) => ip.fmt(f),
        }
    }
}
