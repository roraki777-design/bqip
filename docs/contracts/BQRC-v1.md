# BQRC v1 binary layout

Normative source for this implementation: AC-001 I and ADR-001.

| Header field | Size | Representation |
|---|---:|---|
| magic | 4 | ASCII `BQRC` |
| format_version | 4 | uint32 big-endian, value 1 |
| segment_id | 16 | UUID bytes, network/RFC byte order |

| Record field | Size | Representation |
|---|---:|---|
| metadata_length | 4 | uint32 big-endian |
| payload_length | 4 | uint32 big-endian |
| metadata | metadata_length | Serialized bqip.v1.RawCaptureEvent |
| payload | payload_length | Exact application message bytes |
| checksum | 4 | CRC32C(metadata concatenated with payload), big-endian |

There is no padding or alignment between records. Empty application payloads are
valid. Zero-byte metadata is structurally representable by the framing layer but
does not validate as a RawCaptureEvent; these are distinct validation boundaries.
Frame reading stops on errors instead of attempting resynchronization inside
arbitrary payload bytes.

The segment hash covers all file bytes, including header, lengths and checksums.
It is not stored recursively inside the file it hashes. `verify_segment` accepts
the externally retained expected hash; it detects whole-record deletion which
per-record CRC cannot identify. Consumers must not infer original completeness
from a successful structural scan alone.

Local write policy always flushes and fsyncs the file before rename, then fsyncs
the directory. It is implemented for Linux/POSIX filesystems, not certified for
all network filesystems or Windows-native durability semantics.

Recovery inputs must be closed and quiescent. A recovery report contains source
segment ID, valid prefix offset, count, state and failure/offset. A recovered copy
has a new segment ID and retains original metadata and payload bytes. Retain the
report and source as evidence; no deletion or remote publication happens here.
