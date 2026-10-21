use crate::{config::Config, hash, Error, Result};
use bqip_contracts::{
    hex, manifests,
    metadata::{MetadataCatalog, MetadataResolution, ResolverMode},
    proto::*,
};
use prost::Message;
use serde_json::{json, Value};

/// Pins an immutable accepted catalog. The resolver remains bqip-contracts' resolver.
pub struct Catalog {
    catalog: MetadataCatalog,
    digest: String,
}
impl Catalog {
    pub fn new(versions: Vec<InstrumentMetadataVersion>, config: &Config) -> Result<Self> {
        Self::from_catalog(MetadataCatalog::new(versions)?, config)
    }
    pub fn from_catalog(catalog: MetadataCatalog, config: &Config) -> Result<Self> {
        let versions = catalog.versions();
        if versions.len() > config.max_metadata_versions
            || versions
                .iter()
                .try_fold(0usize, |s, v| s.checked_add(v.encoded_len()))
                .is_none_or(|n| n > config.max_metadata_bytes)
        {
            return Err(Error::CapacityExceeded);
        }
        let mut sorted: Vec<_> = versions.iter().collect();
        sorted.sort_by(|a, b| {
            (&a.instrument_id, &a.metadata_version).cmp(&(&b.instrument_id, &b.metadata_version))
        });
        let semantic: Vec<_> = sorted.into_iter().map(semantic_metadata).collect();
        let digest = hash(&manifests::canonical_bytes(
            &json!({"schema":"BQIP-REPLAY-METADATA-CATALOG-V1", "versions":semantic}),
        )?);
        Ok(Self { catalog, digest })
    }
    pub fn digest(&self) -> &str {
        &self.digest
    }
    pub fn versions(&self) -> &[InstrumentMetadataVersion] {
        self.catalog.versions()
    }
    /// AsLived is metadata-only, not permission to label a replay forensic.
    pub fn resolve(
        &self,
        instrument: &str,
        event_time: Option<i64>,
        original_receive: i64,
        mode: ResolverMode,
    ) -> Result<MetadataResolution<'_>> {
        let time = event_time.ok_or(Error::MetadataEventTimeMissing)?;
        self.catalog
            .resolve_with_basis(
                instrument,
                time,
                mode,
                if mode == ResolverMode::AsLived {
                    Some(original_receive)
                } else {
                    None
                },
            )
            .map_err(Error::from)
    }
}
fn decimal(v: &Option<DecimalValue>) -> Value {
    v.as_ref().map_or(
        Value::Null,
        |d| json!({"coefficient":d.coefficient,"scale":d.scale}),
    )
}
pub fn semantic_metadata(v: &InstrumentMetadataVersion) -> Value {
    // Every accepted field is represented; absence differs from explicit zero/empty.
    let source = v.source.as_ref().map(|s| json!({
        "lineage": s.lineage.as_ref().map(|l| json!({
            "venue_profile_id":l.venue_profile_id,"venue_profile_hash":l.venue_profile_hash.as_deref().map(hex),
            "capture_id":l.capture_id.as_ref().map(|c| json!({"collector_instance_id":c.collector_instance_id.as_deref().map(hex),"capture_seq":c.capture_seq.map(|n|n.to_string())})),
            "raw_content_hash":l.raw_content_hash.as_deref().map(hex),"receive_timestamp_ns":l.receive_timestamp_ns.map(|n|n.to_string())
        })),
        "transport":s.transport,"method":s.method,"endpoint":s.endpoint,"http_status":s.http_status,
        "query_parameters":s.query_parameters.iter().map(|p|json!({"name":p.name,"value":p.value})).collect::<Vec<_>>()
    }));
    json!({
        "instrument_id":v.instrument_id,"metadata_version":v.metadata_version,
        "effective_from_ns":v.effective_from_ns.map(|n|n.to_string()),"effective_to_ns":v.effective_to_ns.map(|n|n.to_string()),
        "known_from_ns":v.known_from_ns.map(|n|n.to_string()),"known_to_ns":v.known_to_ns.map(|n|n.to_string()),
        "supersedes_version":v.supersedes_version,"contract_multiplier":decimal(&v.contract_multiplier),"quantity_unit":v.quantity_unit,
        "tick_size":decimal(&v.tick_size),"lot_size":decimal(&v.lot_size),"product_type":v.product_type,
        "margin_asset":v.margin_asset,"settlement_asset":v.settlement_asset,"effective_time_basis":v.effective_time_basis,
        "venue":v.venue,"product_family":v.product_family,"symbol":v.symbol,"pair":v.pair,"contract_type":v.contract_type,
        "base_asset":v.base_asset,"quote_asset":v.quote_asset,"venue_status":v.venue_status,"source":source,
        "minimum_quantity":decimal(&v.minimum_quantity),"maximum_quantity":decimal(&v.maximum_quantity)
    })
}
