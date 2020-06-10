
use tiberius::Config;
use tokio_util::compat::Tokio02AsyncWriteCompatExt;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
        "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    );
    let config = Config::from_ado_string(&c_str)?;
    let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    let mut client = tiberius::Client::connect(config, tcp.compat_write()).await?;
    let _ = client.transaction(|handle| Box::pin(async move {
        let _ = handle.execute(
            "INSERT INTO #Test (id) VALUES (@P1), (@P2), (@P3)",
            &[&1i32, &2i32, &3i32],
        )
        .await;

        handle.execute(
            "INSERT INTO #Test (id) VALUES (@P1), (@P2), (@P3)",
            &[&4i32, &5i32, &6i32],
        )
        .await
        })
    ).await?;
    Ok(())
}

