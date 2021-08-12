use byteorder::*;
use chrono::prelude::*;
use chrono::Duration;
use futures::stream::StreamExt;
use isahc::prelude::*;
use isahc::Body;
use isahc::Response;
use regex::Regex;
use std::io::Cursor;

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
    let mut r: Vec<String> = Vec::new();
    let mut from = start;

    while from < end {
        let mut v = build_day_urls(instrument, from);
        r.append(&mut v);
        from = from + Duration::days(1);
    }

    r
}

#[derive(Debug)]
struct UrlInfo {
    symbol: String,
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
}

fn url_info(url: &str) -> Option<UrlInfo> {
    let re =
        Regex::new(r"(?x)(?P<s>[[:upper:]]+)/(?P<y>\d{4})/(?P<m>\d{2})/(?P<d>\d{2})/(?P<h>\d{2})")
            .unwrap();
    for cap in re.captures_iter(url) {
        let symbol = String::from(&cap[1]);
        let year = cap[2].trim().parse::<i32>().unwrap();
        let month = cap[3].trim().parse::<u32>().unwrap() + 1;
        let day = cap[4].trim().parse::<u32>().unwrap();
        let hour = cap[5].trim().parse::<u32>().unwrap();
        return Some(UrlInfo {
            symbol,
            year,
            month,
            day,
            hour,
        });
    }
    None
}

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

fn process_response(url: &str, mut response: Response<Body>) -> Vec<Record>{
    assert!(response.status() == 200 || response.status() == 404);
    println!("{} --> {}", url, response.status());

    let mut buf = vec![];
    response.copy_to(&mut buf).unwrap();

    let mut decomp: Vec<u8> = Vec::new();
    lzma_rs::lzma_decompress(&mut buf.as_slice(), &mut decomp).unwrap();

    let info = url_info(url).unwrap();

    let decomp_len = decomp.len();
    let mut rdr = Cursor::new(decomp);

    let mut records:Vec<Record> = Vec::new();
    let mut pos: usize = 0;
    while pos < decomp_len {
        let ms = rdr.read_i32::<BigEndian>().unwrap();
        let dt_start = Utc
            .ymd(info.year, info.month, info.day)
            .and_hms(info.hour, 0, 0);
        let dt = dt_start + Duration::milliseconds(ms as i64);
        let ask = rdr.read_i32::<BigEndian>().unwrap() as f32 / 100000.0;
        let bid = rdr.read_i32::<BigEndian>().unwrap() as f32 / 100000.0;
        let ask_vol = rdr.read_f32::<BigEndian>().unwrap();
        let bid_vol = rdr.read_f32::<BigEndian>().unwrap();
        records.push(Record::new(dt,ask,bid,ask_vol,bid_vol));
        println!("{} {} {} {} {}", dt, ask, bid, ask_vol, bid_vol);
        pos += 20;
    }
    records
}

// 返回出错的URL
async fn download_urls(urls: Vec<String>) -> Vec<String> {
    let fetches = futures::stream::iter(urls.into_iter().map(|url| async move {
        let backup_url = url.clone();
        let response = isahc::get(url);

        if response.is_ok() {
            let resp = response.unwrap();
            process_response(&backup_url, resp);

            None
        } else {
            Some(backup_url)
        }
    }))
    .buffer_unordered(100)
    .collect::<Vec<Option<String>>>();

    fetches
        .await
        .into_iter()
        .filter(|value| value.is_some())
        .collect::<Vec<Option<String>>>()
        .into_iter()
        .map(|v| v.unwrap())
        .collect::<Vec<String>>()
}

#[tokio::main]
async fn main() {
    /* let start = Utc.ymd(2021, 8, 11);
    let end = Utc.ymd(2021, 8, 12);
    let instrument = "eurusd";

    let urls = build_urls(instrument, start, end);
    let error_urls = download_urls(urls).await;

    println!("erros={:?}", error_urls);
    */
    //let urls = vec!["http://datafeed.dukascopy.com/datafeed/GBPJPY/2012/11/03/00h_ticks.bi5".to_string()];
    let urls =
        vec!["http://datafeed.dukascopy.com/datafeed/EURUSD/2021/07/12/16h_ticks.bi5".to_string()];
    let error_urls = download_urls(urls).await;
    println!("erros={:?}", error_urls);
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
            let opt_info = url_info(&urls[i]);
            assert!(opt_info.is_some());
            let info = opt_info.unwrap();
            assert!(info.symbol == "EURUSD");
            assert!(info.year == 2003);
            assert!(info.month == 0);
            assert!(info.day == 5);
            assert!(info.hour == i as u32);
        }
    }
}
