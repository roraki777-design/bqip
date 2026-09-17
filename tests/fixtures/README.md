# Golden fixture provenance

These files are fixed expected values, not test outputs regenerated from BQIP.

* Decimal coefficients and scales, temporal outcomes, identity preimage hex and
  JCS UTF-8 strings were specified from AC-001 and the documented format.
* SHA-256 expected digests were computed over those independently specified bytes
  using Python's standard hashlib, without importing any BQIP implementation.
* Raw Protobuf tags, lengths and field bytes were independently assembled from
  the IDL. The fixture authoring path used forward CRC32C polynomial 0x1edc6f41
  with bit reversal. The Python implementation uses reflected 0x82f63b78; Rust
  uses the allowed crc32c crate. Both must match the known 123456789 check value.
* complete.bqrc contains TEXT, BINARY, invalid UTF-8, empty, and invalid JSON
  application payloads. None is parsed as market data.
* truncated.bqrc removes two bytes of the last CRC. corrupt.bqrc flips one bit
  of the last CRC. The first four complete frames remain available for diagnosis.
* Frame metadata includes explicitly present zero values, a structured capture
  identity, native UUID bytes and the exact raw payload hash.

Never bless a changed output automatically. Changing these vectors requires an
explanation against the normative contract. Rust and Python, including official
generated Protobuf bindings, independently consume the normative vectors.

Additional completion-directive fixtures:

* manifests/conformance.json: literal RFC 8785 string/ordering expectations for
  the documented integer-only domain, plus rejected inputs. These were derived
  independently of both BQIP implementations; see docs/contracts/JCS-profile.md.
* contracts/enums.json: explicit symbolic/wire names and numeric tags for every
  shared enum, checked against generated contracts and Python domain enums.
* capture/crc32c.json: empty input and the standard 123456789 Castagnoli vector;
  both implementations also check every golden frame's independent CRC footer.
