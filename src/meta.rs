use chrono::offset::TimeZone;
use chrono::{DateTime, Utc};
use colored::Colorize;
use isahc::{config::Configurable, ReadResponseExt, Request, RequestExt};
use serde_json::Value;
use std::collections::HashMap;
use std::io::Write;

use crate::MetaOptions;

#[derive(Debug)]
pub struct InstrumentMeta {
    pub point: f32,
    pub history_start_tick: DateTime<Utc>,
}

fn download_and_retry(opt: &MetaOptions) -> Option<String> {
    if opt.verbose {
        print!("{} meta info", "Fetching..".yellow());
    }
    let mut index: u16 = 0;
    while index < opt.retry_count {
        let meta_data = fetch_meta_data();
        if meta_data.is_some() {
            if opt.verbose {
                println!("  {}", "Done".green());
            }
            return meta_data;
        }
        index += 1;
    }
    println!("  {}", "Error".red());
    None
}

pub fn download_meta_info(opt: &MetaOptions) {
    let meta_data = download_and_retry(opt);
    if meta_data.is_none() {
        return;
    }

    if opt.verbose {
        print!(
            "{} {}",
            "Writing...".yellow(),
            opt.output.as_path().to_str().unwrap()
        );
    }

    let json = std::fs::File::create(&opt.output);
    if json.is_err() {
        println!("  {} to create file", "Error".red());
        return;
    }
    let mut json = json.unwrap();
    
    let write_result = json.write_all(meta_data.unwrap().as_bytes());
    if write_result.is_err() {
        println!("  {} to write file", "Error".red());
        return;
    }

    if opt.verbose {
        println!("  {}", "Done".green());
    }
}

pub fn build_meta_info() -> HashMap<String, InstrumentMeta> {
    let mut info_map = HashMap::new();
    let meta_data = fetch_meta_data();
    if meta_data.is_some() {
        let config = meta_data.as_ref().unwrap();
        let all: Value = serde_json::from_str(&config).unwrap();
        let instruments = all["instruments"].as_object().clone().unwrap();
        for (k, v) in instruments {
            let dict = v.as_object().unwrap();
            let pip_opt = dict["pipValue"].as_f64();
            let start_opt = dict["history_start_tick"].as_str();

            if !(pip_opt.is_none() || start_opt.is_none()) {
                let point = 10.0 / (pip_opt.unwrap() as f32);
                let start = Utc.timestamp_millis(start_opt.unwrap().parse::<i64>().unwrap());
                let info = InstrumentMeta {
                    point,
                    history_start_tick: start,
                };
                let key = k.clone().replace("/", "");
                info_map.insert(key, info);
            }
        }
    }
    info_map
}

pub fn fetch_meta_data() -> Option<String> {
    // 10 / pipValue
    let response =
        Request::get("https://freeserv.dukascopy.com/2.0/index.php?path=common%2Finstruments")
            .header("referer", "https://freeserv.dukascopy.com/")
            .timeout(std::time::Duration::from_secs(5))
            .body(())
            .unwrap()
            .send();

    if response.is_ok() {
        let mut resp = response.unwrap();
        if resp.status() == 200 {
            let meta_result = resp.text();

            if meta_result.is_ok() {
                let meta_data_jsonp = meta_result.unwrap();
                let meta_data = meta_data_jsonp[6..meta_data_jsonp.len() - 1].to_string();
                return Some(meta_data);
            }
        }
    }
    None
}
