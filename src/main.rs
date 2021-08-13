use byteorder::*;
use chrono::prelude::*;
use chrono::Duration;
use futures::stream::StreamExt;
use isahc::prelude::*;
use isahc::Body;
use isahc::Response;
use lazy_regex::regex_captures;
use std::fs::File;
use std::io::Cursor;
use std::io::Write;
use std::mem::size_of;

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
struct UrlInfo {
    symbol: String,
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
}

#[derive(Debug)]
enum DownloadResult {
    Records(Vec<Record>),
    Error(String),
}

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

fn process_response(url: &str, mut response: Response<Body>) -> Vec<Record> {
    println!("StatusCode:{}", response.status());
    assert!(response.status() == 200 || response.status() == 404);
    println!("{} --> {}", url, response.status());

    let mut records: Vec<Record> = Vec::new();

    if response.status() == 404 {
        return records;
    }

    if response.body().len().unwrap() == 0 {
        return records;
    }

    let mut buf = vec![];
    response.copy_to(&mut buf).unwrap();

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
    records
}

// 返回出错的URL
async fn download_urls(urls: Vec<String>) -> Vec<DownloadResult> {
    let fetches = futures::stream::iter(urls.into_iter().map(|url| async move {
        let backup_url = url.clone();
        let response = isahc::get(url);

        if response.is_ok() {
            let resp = response.unwrap();
            let records = process_response(&backup_url, resp);
            DownloadResult::Records(records)
        } else {
            DownloadResult::Error(backup_url)
        }
    }))
    .buffer_unordered(24*15)
    .collect::<Vec<DownloadResult>>();

    fetches.await
    //.into_iter()
    //.filter(|value| value.is_some())
    //.map(|v| v.unwrap())
    //.collect::<Vec<String>>()
}

#[tokio::main]
async fn main() {
    let start = Utc.ymd(2003, 5, 5);
    let end = Utc.ymd(2003, 11, 3);
    let instrument = "eurusd";

    let urls = build_urls(instrument, start, end);
    println!("urls {}", urls.len());
    let result = download_urls(urls).await;

    let mut error_urls: Vec<String> = Vec::new();
    let mut records: Vec<Record> = Vec::new();
    for item in result {
        match item {
            DownloadResult::Records(record) => records.extend(record),
            DownloadResult::Error(url) => {
                error_urls.push(url);
            }
        }
    }

    records.sort_by(|a, b| a.dt.cmp(&b.dt));

    //for v in records {
    //    println!("{} {} {} {} {}", v.dt, v.ask, v.bid, v.ask_vol, v.bid_vol);
    //}

    println!("erros={:?}", error_urls);
    println!("record size = {}", size_of::<Record>());

    let mut csvfile = File::create("eurusd.csv").unwrap();

    csvfile.write("datetime,ask,bid,ask_vol,bid_vol\n".as_bytes()).unwrap();

    for v in records {
        let r = format!("{},{},{},{},{}\n", v.dt, v.ask, v.bid, v.ask_vol, v.bid_vol);
        csvfile.write(r.as_bytes()).unwrap();
    }
    csvfile.flush().unwrap();
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
