use once_cell::sync::Lazy;

static XDS_CONFIG: Lazy<serde_json::Value> = Lazy::new(|| {
    serde_json::json!({
        "clusters": {
            "name": "cluster-a",
            "endpoints": [{
                "ip": "127.0.0.1",
                "port": 8000
            }]
        },
        "filterchain": {
            "name": "quilkin.extensions.filters.debug.v1alpha1.Debug",
            "typed_config": {
                "@type": "quilkin.extensions.filters.debug.v1alpha1.Debug",
                "id": "hello"
            }
        }
    })
});

static XDS_CONFIG_FILE: Lazy<tempfile::NamedTempFile> = Lazy::new(|| {
    use std::io::Write;
    let file = tempfile::NamedTempFile::new().unwrap();
    write!(&file, "{}", serde_yaml::to_string(&*XDS_CONFIG).unwrap()).unwrap();
    file
});

static MANAGEMENT_SERVER: Lazy<()> = Lazy::new(|| {
    std::thread::spawn(|| {
        std::process::Command::new("go")
            .args(&[
                "run",
                "cmd/file/file.go",
                "--config",
                &XDS_CONFIG_FILE.path().display().to_string(),
            ])
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
        .with_dynamic(["http://localhost:18000".into()])
        .build();

    tokio::spawn(quilkin::run_with_config(
        std::sync::Arc::new(config),
        vec![],
    ));

    let mut new_config = XDS_CONFIG.clone();
    new_config["filterchain"]["typed_config"]["id"] = "goodbye".into();

    std::fs::write(XDS_CONFIG_FILE.path(), serde_yaml::to_string(&new_config)?)?;

    Ok(())
}
