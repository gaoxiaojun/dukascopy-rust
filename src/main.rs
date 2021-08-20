mod downloader;
mod meta;
use chrono::prelude::*;
use chrono::Duration;
use colored::Colorize;
use meta::download_meta_info;
use std::io::Write;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(
    name = "DukascopyCLI",
    about = "An cli tool to download/merge/aggregator dukascopy tick data."
)]
enum Opt {
    /// Download Meta Info
    Meta(MetaOptions),

    /// Download Tick Data
    Download(DownloadOptions),

    /// Merge Separate Tick Data Files
    Merge(MergeOptions),

    /// Aggregator Tick Data To Candle
    Aggregator(AggregatorOptions),
}

#[derive(StructOpt, Debug)]
pub struct MetaOptions {
    /// Verbose mode
    #[structopt(short, long)]
    verbose: bool,

    /// Output Meta File
    #[structopt(short, long, parse(from_os_str))]
    output: PathBuf,

    /// Retry Count
    #[structopt(short, long, default_value = "3")]
    retry_count: u16,
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
    #[structopt(short, long, parse(from_os_str), default_value = "bi5")]
    input: PathBuf,

    /// Output Direction
    #[structopt(short, long, parse(from_os_str), default_value = "bi5")]
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

    /// Source Tick Data File
    #[structopt(short, long, parse(from_os_str))]
    input: PathBuf,

    /// Output Aggregator Csv File
    #[structopt(short, long, parse(from_os_str))]
    output: PathBuf,

    /// Aggregator Timeframe, like 15s 1m 1h 1d 1w 1m 1y
    #[structopt(short, long)]
    timeframe: String,
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

fn command_merge(opt: &MergeOptions) -> std::io::Result<()> {
    for symbol in &opt.symbols {
        if !opt.input.is_dir() {
            return Ok(());
        }

        let mut output_path = opt.output.clone();
        output_path.push(format!("{}.csv", symbol.to_uppercase()));
        let mut csv_file = std::fs::File::create(&output_path)?;
        let mut bi5_files: Vec<PathBuf> = Vec::new();

        let mut input_path = opt.input.clone();
        input_path.push(symbol.to_uppercase());
        for entry in std::fs::read_dir(&input_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                bi5_files.push(path);
            }
        }

        bi5_files.sort();

        println!("{} {} files", "Find".yellow(), bi5_files.len());

        for path in &bi5_files {
            if opt.verbose {
                println!("{} {}", "Reading".yellow(), path.to_str().unwrap());
            }
            let mut file = std::fs::File::open(path)?;
            std::io::copy(&mut file, &mut csv_file)?;
            csv_file.write("\n".as_bytes())?;
        }
        csv_file.flush()?;

        println!(
            "{} {} {}",
            "Written".yellow(),
            output_path.to_str().unwrap(),
            "Done".green()
        );
    }

    Ok(())
}

fn command_aggregator(opt: &AggregatorOptions) -> std::io::Result<()> {
    Ok(())
}

fn command_meta(opt: &MetaOptions) -> std::io::Result<()> {
    download_meta_info(opt);

    Ok(())
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let options = Opt::from_args();

    match options {
        Opt::Download(opt) => {
            let _ = command_download(&opt).await;
        }
        Opt::Merge(opt) => {
            let _ = command_merge(&opt);
        }
        Opt::Aggregator(opt) => {
            let _ = command_aggregator(&opt);
        }
        Opt::Meta(opt) => {
            let _ = command_meta(&opt);
        }
    }
    Ok(())
}
