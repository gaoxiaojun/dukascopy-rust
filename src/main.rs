mod downloader;
mod meta;
use chrono::prelude::*;
use chrono::Duration;
use colored::Colorize;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(
    name = "DukascopyCLI",
    about = "An cli tool to download/merge/aggregator dukascopy ticker data."
)]
enum Opt {
    Download(DownloadOptions),
    Merge(MergeOptions),
    Aggregator(AggregatorOptions),
}

#[derive(StructOpt, Debug)]
struct DownloadOptions {
    /// Verbose mode
    #[structopt(short, long)]
    verbose: bool,

    /// Output Direction
    #[structopt(short, long, parse(from_os_str), default_value = "bi5")]
    output: PathBuf,

    /// Symbols like EURUSD GBPUSD, split by whitespace
    #[structopt(name = "SYMBOLS")]
    symbols: Vec<String>,

    /// Start Date, [default: Today - 1]
    #[structopt(short, long)]
    start: Option<NaiveDate>,

    /// End Date, [default: Today]
    #[structopt(short, long)]
    end: Option<NaiveDate>,

    /// Retry Count
    #[structopt(short, long, default_value = "10")]
    retry_count: u16,
}

#[derive(StructOpt, Debug)]
struct MergeOptions {
    /// Verbose mode
    #[structopt(short, long)]
    verbose: bool,

    /// Source Direction
    #[structopt(short, long, parse(from_os_str))]
    input: PathBuf,

    /// Output Direction
    #[structopt(short, long, parse(from_os_str))]
    output: PathBuf,

    /// Symbols like EURUSD GBPUSD, split by whitespace
    #[structopt(name = "SYMBOLS")]
    symbols: Vec<String>,
}

#[derive(StructOpt, Debug)]
struct AggregatorOptions {
    /// Verbose mode
    #[structopt(short, long)]
    verbose: bool,

    /// Output Direction
    #[structopt(short, long, parse(from_os_str))]
    output: PathBuf,

    /// Symbols like EURUSD GBPUSD, split by whitespace
    #[structopt(name = "SYMBOLS")]
    symbols: Vec<String>,

    /// Start Date, [default: Today - 1]
    #[structopt(short, long)]
    start: Option<NaiveDate>,

    /// End Date, [default: Today]
    #[structopt(short, long)]
    end: Option<NaiveDate>,
}

async fn command_download(opt: &DownloadOptions) -> std::io::Result<()> {
    print!("{} instruments meta info...", "Fetching".yellow());
    let meta_dict = meta::build_meta_info();

    if meta_dict.len() > 0 {
        println!("{}", "Done".green());
    } else {
        println!("{}", "Error!".red());
        return Ok(());
    }

    let mut error_symbols: Vec<String> = Vec::new();
    for symbol in &opt.symbols {
        if !meta_dict.contains_key(&symbol.to_uppercase()) {
            error_symbols.push(symbol.clone());
        }
    }

    if error_symbols.len() > 0 {
        println!(
            "{}:Cannot found symbol {}",
            "Error".red(),
            error_symbols.join(",").yellow()
        );
        return Ok(());
    }

    let start = match opt.start {
        None => (Utc::now() - Duration::days(1)).date(),
        Some(nd) => Date::<Utc>::from_utc(nd, Utc),
    };

    let end = match opt.end {
        None => Utc::now().date(),
        Some(nd) => Date::<Utc>::from_utc(nd, Utc),
    };

    for symbol in &opt.symbols {
        let info = &meta_dict[symbol];
        let start_date = if start < info.history_start_tick.date() {
            info.history_start_tick.date()
        } else {
            start
        };

        let _ = downloader::download(
            &meta_dict,
            symbol,
            &opt.output,
            start_date,
            end,
            opt.retry_count,
            opt.verbose,
        )
        .await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let options = Opt::from_args();

    match options {
        Opt::Download(opt) => {
            let _ = command_download(&opt).await;
        }
        Opt::Merge(_opt) => {}
        Opt::Aggregator(_opt) => {}
    }
    Ok(())
}
