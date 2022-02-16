use once_cell::sync::Lazy;

static MANAGEMENT_SERVER: Lazy<()> = Lazy::new(|| {
    std::thread::spawn(|| {
        std::process::Command::new("go")
            .args(&["run", "cmd/file/file.go"])
            .current_dir(std::env::current_dir()?.join("xds"))
            .output()
    });
});

#[tokio::test]
async fn filter_chain_updates() -> Result<(), Box<dyn std::error::Error>> {
    Lazy::force(&MANAGEMENT_SERVER);
    std::thread::sleep(std::time::Duration::from_secs(2));
    tracing_subscriber::fmt().pretty().init();

    let config = quilkin::config::Builder::empty()
        .with_port(8000)
        .with_dynamic([
            "http://localhost:18000".into(),
        ])
        .build();

    quilkin::run_with_config(std::sync::Arc::from(config), vec![]).await?;
    Ok(())
}
