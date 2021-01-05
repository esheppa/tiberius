mod auth;
mod config;
mod connection;
mod tls;

pub use auth::*;
pub use config::*;
pub(crate) use connection::*;

use crate::{
    result::{ExecuteResult, QueryResult},
    tds::{
        codec::{self, RpcStatusFlags},
        stream::TokenStream,
    },
    SqlReadBytes, ToSql,
};
use codec::{BatchRequest, ColumnData, PacketHeader, RpcParam, RpcProcId, TokenRpcRequest};
use futures::{AsyncRead, AsyncWrite};
use core::future;
use std::{borrow::Cow, fmt::Debug};

/// `Client` is the main entry point to the SQL Server, providing query
/// execution capabilities.
///
/// A `Client` is created using the [`Config`], defining the needed
/// connection options and capabilities.
///
/// # Example
///
/// ```no_run
/// # use tiberius::{Config, AuthMethod};
/// # use tokio_util::compat::TokioAsyncWriteCompatExt;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut config = Config::new();
///
/// config.host("0.0.0.0");
/// config.port(1433);
/// config.authentication(AuthMethod::sql_server("SA", "<Mys3cureP4ssW0rD>"));
///
/// let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
/// tcp.set_nodelay(true)?;
/// // Client is ready to use.
/// let client = tiberius::Client::connect(config, tcp.compat_write()).await?;
/// # Ok(())
/// # }
/// ```
///
/// [`Config`]: struct.Config.html
// #[derive(Debug)]
pub struct Client<'a> {
    send: futures::channel::mpsc::UnboundedSender<QueryParts<'a>>,
}

type QueryParts<'a> = (Cow<'a, str>, &'a [&'a dyn ToSql], futures::channel::mpsc::UnboundedSender<crate::Result<crate::tds::stream::ReceivedToken>>);

pub struct DbConnection<'a, S: AsyncRead + AsyncWrite + Unpin + Send> {
    recv: futures::channel::mpsc::UnboundedReceiver<QueryParts<'a>>,
    connection: Connection<S>,
}

impl<'a, S: 'a + AsyncRead + AsyncWrite + Unpin + Send> DbConnection<'a, S> {
    pub async fn run(self) {
        use futures::StreamExt;

        let DbConnection { mut recv, mut connection } = self;

        while let Some((query, params, sender)) = recv.next().await {
            if let Err(e) = connection.flush_stream().await {
                // send err down the channel
            }

            // let mut rpc_params = Self::rpc_params(query);
            let mut rpc_params =  vec![
                RpcParam {
                    name: Cow::Borrowed("stmt"),
                    flags: RpcStatusFlags::empty(),
                    value: ColumnData::String(Some(query.into())),
                },
                RpcParam {
                    name: Cow::Borrowed("params"),
                    flags: RpcStatusFlags::empty(),
                    value: ColumnData::I32(Some(0)),
                },
            ];

            let mut param_str = String::new();

            for (i, param) in params.iter().enumerate() {
                if i > 0 {
                    param_str.push(',')
                }
                param_str.push_str(&format!("@P{} ", i + 1));
                let param_data = param.to_sql();
                param_str.push_str(&param_data.type_name());
    
                rpc_params.push(RpcParam {
                    name: Cow::Owned(format!("@P{}", i + 1)),
                    flags: RpcStatusFlags::empty(),
                    value: param_data,
                });
            }
    
            if let Some(params) = rpc_params.iter_mut().find(|x| x.name == "params") {
                params.value = ColumnData::String(Some(param_str.into()));
            }
    
            let req = TokenRpcRequest::new(
                RpcProcId::SpExecuteSQL,
                rpc_params,
                connection.context().transaction_descriptor(),
            );
    
            let id = connection.context_mut().next_packet_id();
            if let Err(e) = connection.send(PacketHeader::rpc(id), req)
                .await {
                // send err down the channel
            }


            let mut ts = TokenStream::new(&mut connection).try_unfold();
            while let Some(recv_token) = ts.next().await {
                if let Err(e) = sender.unbounded_send(recv_token) {
                    // probs panic!
                }
            }

        }
    }
}

impl<'a> Client<'a> {
    /// Uses an instance of [`Config`] to specify the connection
    /// options required to connect to the database using an established
    /// tcp connection
    ///
    /// [`Config`]: struct.Config.html
    pub async fn connect<S: AsyncRead + AsyncWrite + Unpin + Send>(config: Config, tcp_stream: S) -> crate::Result<(Client<'a>, DbConnection<'a, S>)> 
    {
        let connection = Connection::connect(config, tcp_stream).await?;
        let (tx_query, rx_query) = futures::channel::mpsc::unbounded();
        Ok((
            Client { send: tx_query },
            DbConnection { recv: rx_query, connection },
        ))
    }

    // /// Executes SQL statements in the SQL Server, returning the number rows
    // /// affected. Useful for `INSERT`, `UPDATE` and `DELETE` statements. The
    // /// `query` can define the parameter placement by annotating them with
    // /// `@PN`, where N is the index of the parameter, starting from `1`. If
    // /// executing multiple queries at a time, delimit them with `;` and refer to
    // /// [`ExecuteResult`] how to get results for the separate queries.
    // ///
    // /// For mapping of Rust types when writing, see the documentation for
    // /// [`ToSql`]. For reading data from the database, see the documentation for
    // /// [`FromSql`].
    // ///
    // /// # Example
    // ///
    // /// ```no_run
    // /// # use tiberius::Config;
    // /// # use tokio_util::compat::TokioAsyncWriteCompatExt;
    // /// # use std::env;
    // /// # #[tokio::main]
    // /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // /// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
    // /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    // /// # );
    // /// # let config = Config::from_ado_string(&c_str)?;
    // /// # let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
    // /// # tcp.set_nodelay(true)?;
    // /// # let mut client = tiberius::Client::connect(config, tcp.compat_write()).await?;
    // /// let results = client
    // ///     .execute(
    // ///         "INSERT INTO ##Test (id) VALUES (@P1), (@P2), (@P3)",
    // ///         &[&1i32, &2i32, &3i32],
    // ///     )
    // ///     .await?;
    // /// # Ok(())
    // /// # }
    // /// ```
    // ///
    // /// [`ExecuteResult`]: struct.ExecuteResult.html
    // /// [`ToSql`]: trait.ToSql.html
    // /// [`FromSql`]: trait.FromSql.html
    // pub async fn execute<'a>(
    //     &mut self,
    //     query: impl Into<Cow<'a, str>>,
    //     params: &[&dyn ToSql],
    // ) -> crate::Result<ExecuteResult> {
    //     self.connection.flush_stream().await?;
    //     let rpc_params = Self::rpc_params(query);

    //     self.rpc_perform_query(RpcProcId::SpExecuteSQL, rpc_params, params)
    //         .await?;

    //     Ok(ExecuteResult::new(&mut self.connection).await?)
    // }

    /// Executes SQL statements in the SQL Server, returning resulting rows.
    /// Useful for `SELECT` statements. The `query` can define the parameter
    /// placement by annotating them with `@PN`, where N is the index of the
    /// parameter, starting from `1`. If executing multiple queries at a time,
    /// delimit them with `;` and refer to [`QueryResult`] on proper stream
    /// handling.
    ///
    /// For mapping of Rust types when writing, see the documentation for
    /// [`ToSql`]. For reading data from the database, see the documentation for
    /// [`FromSql`].
    ///
    /// # Example
    ///
    /// ```
    /// # use tiberius::Config;
    /// # use tokio_util::compat::TokioAsyncWriteCompatExt;
    /// # use std::env;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
    /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    /// # );
    /// # let config = Config::from_ado_string(&c_str)?;
    /// # let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
    /// # tcp.set_nodelay(true)?;
    /// # let mut client = tiberius::Client::connect(config, tcp.compat_write()).await?;
    /// let stream = client
    ///     .query(
    ///         "SELECT @P1, @P2, @P3",
    ///         &[&1i32, &2i32, &3i32],
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`QueryResult`]: struct.QueryResult.html
    /// [`ToSql`]: trait.ToSql.html
    /// [`FromSql`]: trait.FromSql.html
    pub async fn query<'conn>(
        &'conn mut self,
        query: impl Into<Cow<'a, str>>,
        params: &'a [&'a dyn ToSql],
    ) -> crate::Result<QueryResult<'conn>>
    where
        'conn: 'a,
    {
        use futures::SinkExt;
        use futures::StreamExt;

        let (tx_result, rx_result) = futures::channel::mpsc::unbounded();

        self.send.send((query.into(), params, tx_result)).await.unwrap();

        let mut result = QueryResult::new(rx_result.boxed());
        result.fetch_metadata().await?;

        Ok(result)
    }

    // /// Execute multiple queries, delimited with `;` and return multiple result
    // /// sets; one for each query.
    // ///
    // /// # Example
    // ///
    // /// ```
    // /// # use tiberius::Config;
    // /// # use tokio_util::compat::TokioAsyncWriteCompatExt;
    // /// # use std::env;
    // /// # #[tokio::main]
    // /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // /// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
    // /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    // /// # );
    // /// # let config = Config::from_ado_string(&c_str)?;
    // /// # let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
    // /// # tcp.set_nodelay(true)?;
    // /// # let mut client = tiberius::Client::connect(config, tcp.compat_write()).await?;
    // /// let row = client.simple_query("SELECT 1 AS col").await?.into_row().await?.unwrap();
    // /// assert_eq!(Some(1i32), row.get("col"));
    // /// # Ok(())
    // /// # }
    // /// ```
    // ///
    // /// # Warning
    // ///
    // /// Do not use this with any user specified input. Please resort to prepared
    // /// statements using the [`query`] method.
    // ///
    // /// [`query`]: #method.query
    // pub async fn simple_query<'a, 'b>(
    //     &'a mut self,
    //     query: impl Into<Cow<'b, str>>,
    // ) -> crate::Result<QueryResult<'a>>
    // where
    //     'a: 'b,
    // {
    //     self.connection.flush_stream().await?;

    //     let req = BatchRequest::new(query, self.connection.context().transaction_descriptor());

    //     let id = self.connection.context_mut().next_packet_id();
    //     self.connection.send(PacketHeader::batch(id), req).await?;

    //     let ts = TokenStream::new(&mut self.connection);
    //     let mut result = QueryResult::new(ts.try_unfold());

    //     result.fetch_metadata().await?;

    //     Ok(result)
    // }


}

