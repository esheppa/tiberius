use futures::TryStreamExt;
use tiberius::{AuthMethod, Client};

use tokio_util::compat::{self, Tokio02AsyncWriteCompatExt};
use tokio::{io, net};



#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mut builder = Client::<compat::Compat<net::TcpStream>>::builder();
    builder.host("localhost");
    builder.port(1433);
    builder.database("master");
    builder.authentication(AuthMethod::sql_server("SA", "<YourStrong@Passw0rd>"));
    builder.trust_cert();

    let mut conn = builder.build(tiberius_tokio::connector).await?;
    let stream = conn.query("SELECT @P1", &[&1]).await?;

    let rows: Vec<_> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await?;

    assert_eq!(1, rows[0]);

    Ok(())
}
