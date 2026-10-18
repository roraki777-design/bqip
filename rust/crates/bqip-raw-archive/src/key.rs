use bqip_binance_collector::capture::Lane;
use uuid::Uuid;
pub fn lane_slug(lane: Lane) -> &'static str {
    match lane {
        Lane::Depth => "depth_ws",
        Lane::Aggtrade => "aggtrade_ws",
        Lane::Rest => "rest",
    }
}
pub fn keys(prefix: &str, lane: Lane, id: Uuid) -> crate::Result<[String; 3]> {
    if prefix.len() > 1024 || !prefix.split('/').all(crate::config::component) {
        return Err(crate::Error::new(
            crate::Kind::Configuration,
            "unsafe archive prefix",
        ));
    }
    let root = format!(
        "{prefix}/raw/v1/binance/usd_m_futures/btcusdt/{}/{id}",
        lane_slug(lane)
    );
    Ok([
        format!("{root}/capture.bqrc"),
        format!("{root}/seal.json"),
        format!("{root}/manifest.json"),
    ])
}
