mod downloader;

use chrono::prelude::*;
use chrono::Duration;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(
    name = "DukascopyTickerDownloader",
    about = "An cli tool to download dukascopy ticker data."
)]
struct Opt {
    /// Verbose mode
    #[structopt(short, long)]
    verbose: bool,

    /// Output Direction
    #[structopt(short, long, parse(from_os_str), default_value = "bi5")]
    output: PathBuf,

    /// Start Date, [default: Today - 1]
    #[structopt(short, long)]
    start: Option<NaiveDate>,

    /// End Date, [default: Today]
    #[structopt(short, long)]
    end: Option<NaiveDate>,

    /// Retry Count
    #[structopt(short, long, default_value = "10")]
    retry_count: u8,

    /// Symbols like EURUSD,GBPUSD
    #[structopt(name = "SYMBOL")]
    symbols: Vec<String>,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let opt = Opt::from_args();

    let start = match opt.start {
        None => (Utc::now() - Duration::days(1)).date(),
        Some(nd) => Date::<Utc>::from_utc(nd, Utc),
    };

    let end = match opt.end {
        None => Utc::now().date(),
        Some(nd) => Date::<Utc>::from_utc(nd, Utc),
    };

    for symbol in opt.symbols {
        let _ = downloader::download(
            &symbol,
            &opt.output,
            start,
            end,
            opt.retry_count,
            opt.verbose,
        )
        .await?;
    }

    Ok(())
}
