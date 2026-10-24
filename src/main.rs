use std::process::ExitCode;

use rust_natsd::config::Config;
use rust_natsd::server;

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "-h" || a == "--help") {
        println!("{}", Config::usage());
        return ExitCode::SUCCESS;
    }
    let config = match Config::from_args(&args) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("rust-natsd: {e}");
            return ExitCode::FAILURE;
        }
    };

    let running = match server::start(config.clone()).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("rust-natsd: failed to start: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!(
        "Starting nats-server ('{}') on {}",
        config.name, running.addr
    );
    println!("Server is ready");

    // Wait for SIGINT or SIGTERM, then shut down gracefully.
    wait_for_signal().await;
    println!("rust-natsd: shutting down");
    match running.shutdown().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("rust-natsd: shutdown error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
