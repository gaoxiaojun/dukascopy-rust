use byteorder::*;
use chrono::prelude::*;
use chrono::Duration;
use colored::Colorize;
use futures::stream::StreamExt;
use isahc::prelude::*;
use isahc::AsyncBody;
use isahc::Response;
use lazy_regex::regex_captures;
use std::io::Cursor;
use std::path::Path;
use tokio::fs;
use tokio::io::AsyncWriteExt;

#[derive(Debug)]
pub struct Record {
    dt: DateTime<Utc>,
    ask: f32,
    bid: f32,
    ask_vol: f32,
    bid_vol: f32,
}

impl Record {
    pub fn new(dt: DateTime<Utc>, ask: f32, bid: f32, ask_vol: f32, bid_vol: f32) -> Self {
        Self {
            dt,
            ask,
            bid,
            ask_vol,
            bid_vol,
        }
    }
}

#[derive(Debug)]
pub struct UrlInfo {
    pub symbol: String,
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
}

const DEFAULT_PATH: &'static str = "bi5";

// dukascopy url模式
// http://datafeed.dukascopy.com/datafeed/{品种}/{年}/{月}/{日}/{小时}h_ticks.bi5
// 年：4位数字，大部分品种从2003年开始
// 月：2位数字，0-11
// 日：2位数字，1-31
// 小时：2位数字，0-23

fn build_day_urls(instrument: &str, dt: Date<Utc>) -> Vec<String> {
    let mut r: Vec<String> = Vec::new();
    let y = dt.year();
    let m = dt.month0();
    let d = dt.day();
    for h in 0..24 {
        let url = format!(
            "http://datafeed.dukascopy.com/datafeed/{}/{}/{:0>width$}/{:0>width$}/{:0>width$}h_ticks.bi5",
            instrument.to_uppercase(),
            y,
            m as usize,
            d,
            h,
            width = 2,
        );
        r.push(url);
    }

    r
}

// 在[start, end)区间内
fn build_urls(instrument: &str, start: Date<Utc>, end: Date<Utc>) -> Vec<String> {
    let mut urls: Vec<String> = Vec::new();
    let mut from = start;

    while from < end {
        let v = build_day_urls(instrument, from);
        urls.extend(v);
        from = from + Duration::days(1);
    }

    urls
}

fn decode_url(url: &str) -> UrlInfo {
    let (_, s, y, m, d, h) = regex_captures!(
        r"(?x)(?P<s>[[:upper:]]+)/(?P<y>\d{4})/(?P<m>\d{2})/(?P<d>\d{2})/(?P<h>\d{2})",
        url
    )
    .unwrap();
    let symbol = String::from(s);
    let year = y.trim().parse::<i32>().unwrap();
    let month = m.trim().parse::<u32>().unwrap() + 1;
    let day = d.trim().parse::<u32>().unwrap();
    let hour = h.trim().parse::<u32>().unwrap();
    UrlInfo {
        symbol,
        year,
        month,
        day,
        hour,
    }
}

async fn write_to_file(info: &UrlInfo, records: &Vec<Record>, path: &Path) -> std::io::Result<()> {
    let filename = format!(
        "{}_{}_{:0>width$}_{:0>width$}_{:0>width$}h_ticks.bi5",
        info.symbol,
        info.year,
        info.month,
        info.day,
        info.hour,
        width = 2
    );

    let mut path_buf = path.to_path_buf();
    path_buf.push(filename);

    let mut csv = fs::File::create(path_buf.as_path()).await?;
    let header = format!("datetime, ask, bid, ask_vol, bid_vol\n");
    csv.write_all(header.as_bytes()).await?;
    let content = records
        .iter()
        .map(|r| format!("{},{},{},{},{}", r.dt, r.ask, r.bid, r.ask_vol, r.bid_vol))
        .collect::<Vec<String>>()
        .join("\n");
    csv.write_all(content.as_bytes()).await?;
    csv.flush().await
}

async fn process_response(
    url: &str,
    mut response: Response<AsyncBody>,
    path: &Path,
) -> std::io::Result<()> {
    //if response.status() != 200 {
        println!("{} --> {}", url, response.status());
    //}

    let mut records: Vec<Record> = Vec::new();

    if response.status() == 200 && response.body().len().unwrap() != 0 {
        let mut buf = vec![];
        response.copy_to(&mut buf).await?;

        let mut decomp: Vec<u8> = Vec::new();
        lzma_rs::lzma_decompress(&mut buf.as_slice(), &mut decomp).unwrap();

        let info = decode_url(url);

        let decomp_len = decomp.len();
        let mut cursor = Cursor::new(decomp);

        let mut pos: usize = 0;

        while pos < decomp_len {
            let ms = cursor.read_i32::<BigEndian>().unwrap();
            let dt_start = Utc
                .ymd(info.year, info.month, info.day)
                .and_hms(info.hour, 0, 0);
            let dt = dt_start + Duration::milliseconds(ms as i64);
            let ask = cursor.read_i32::<BigEndian>().unwrap() as f32 / 100000.0;
            let bid = cursor.read_i32::<BigEndian>().unwrap() as f32 / 100000.0;
            let ask_vol = cursor.read_f32::<BigEndian>().unwrap();
            let bid_vol = cursor.read_f32::<BigEndian>().unwrap();
            records.push(Record::new(dt, ask, bid, ask_vol, bid_vol));
            pos += 20;
        }

        write_to_file(&info, &records, path).await?
    }
    Ok(())
}

// 返回出错的URL
async fn download_urls(urls: Vec<String>, path: &Path) -> Vec<String> {
    let fetches = futures::stream::iter(urls.into_iter().map(|url| async move {
        let backup_url = url.clone();
        let response = isahc::get_async(url).await;

        if response.is_ok() {
            let resp = response.unwrap();
            let _ = process_response(&backup_url, resp, path).await;
            None
        } else {
            Some(backup_url)
        }
    }))
    .buffer_unordered(24 * 15)
    .collect::<Vec<Option<String>>>();

    fetches
        .await
        .into_iter()
        .filter(|value| value.is_some())
        .map(|v| v.unwrap())
        .collect::<Vec<String>>()
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let start = Utc.ymd(2003, 5, 5);
    let end = Utc.ymd(2003, 5, 6);
    let symbol = "eurusd";

    let mut path_buf = std::path::Path::new(&DEFAULT_PATH).to_path_buf();
    path_buf.push(symbol.to_uppercase());

    if !std::path::Path::new(path_buf.as_path()).exists() {
        fs::create_dir_all(path_buf.as_path()).await?;
    }

    println!(
        "Downloading {} from:{} to:{} ---> Write To {}",
        symbol.to_uppercase().as_str().yellow(),
        start.to_string().green(),
        end.to_string().green(),
        path_buf.as_path().to_str().unwrap().yellow()
    );

    let urls = build_urls(symbol, start, end);
    let mut error_urls = download_urls(urls, path_buf.as_path()).await;

    let mut retry_count = 3;
    while error_urls.len() > 0 && retry_count > 0{
        println!("{}", "Retry...".yellow());
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        error_urls = download_urls(error_urls, path_buf.as_path()).await;
        retry_count -= 1;
    }

    if error_urls.len() > 0 {
        println!("Error fetch urls = {:?}", error_urls);
    }

    println!("Done");

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn test_day_urls() {
        let dt = Utc.ymd(2003, 1, 5);
        let v = build_day_urls("eurusd", dt);
        assert!(v.len() == 24);
        assert_eq!(
            v[0],
            "http://datafeed.dukascopy.com/datafeed/EURUSD/2003/00/05/00h_ticks.bi5"
        );
        assert_eq!(
            v.last().unwrap(),
            "http://datafeed.dukascopy.com/datafeed/EURUSD/2003/00/05/23h_ticks.bi5"
        );
    }

    #[test]
    fn test_days() {
        let start = Utc.ymd(2003, 1, 5);
        let end = Utc.ymd(2003, 1, 30);
        let v = build_urls("eurusd", start, end);
        assert!(v.len() == 600);
        assert_eq!(
            v[0],
            "http://datafeed.dukascopy.com/datafeed/EURUSD/2003/00/05/00h_ticks.bi5"
        );
        assert_eq!(
            v.last().unwrap(),
            "http://datafeed.dukascopy.com/datafeed/EURUSD/2003/00/29/23h_ticks.bi5"
        );
    }

    #[test]
    fn test_url_info() {
        let dt = Utc.ymd(2003, 1, 5);
        let urls = build_day_urls("eurusd", dt);
        for i in 0..24 {
            let info = decode_url(&urls[i]);
            assert!(info.symbol == "EURUSD");
            assert!(info.year == 2003);
            assert!(info.month == 1);
            assert!(info.day == 5);
            assert!(info.hour == i as u32);
        }
    }
}
