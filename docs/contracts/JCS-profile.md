# BQIP manifest canonicalization domain

Authority: AC-001 J and Completion Directive #001-A §6. This is an integer-only
subset of RFC 8785, not a general floating-point JCS implementation.

The manifest root must be a JSON object. Nested values may be objects, arrays,
Unicode scalar strings, null, booleans and integer tokens in
[-9007199254740991, 9007199254740991]. Integer negative zero becomes zero.
Decimal points, exponent notation, leading plus, nonfinite numbers and integers
outside the interval fail. Larger exact quantities must be strings. Arrays retain
order. Duplicate keys at any depth and unpaired surrogate escapes fail.

Keys sort recursively by unsigned UTF-16 code units, without locale or Unicode
normalization. Strings retain Unicode scalars; ASCII controls use the prescribed
short escapes or lowercase hexadecimal escapes. Output is UTF-8 without a BOM or
insignificant whitespace. Only root `manifest_hash` and `comments` are excluded
from the manifest hash. Nested keys with these names remain in the hash. The
complete document, including annotations, must first satisfy this input domain.

`canonical_bytes` is the generic value canonicalizer; `parse_manifest` and
`canonical_manifest` are the object-root manifest boundary.

The external `tests/fixtures/manifests/conformance.json` contains eight accepted
vectors and seventeen rejected inputs. Expected canonical strings were specified
literally from the rules, then encoded and hashed with stdlib tools without any
BQIP import. Tests never regenerate expected outputs. Cases exercise UTF-16 versus
code-point ordering, supplementary characters, property prefixes, controls and
escaping, arrays containing objects, null/bool, integer endpoints, normalization
preservation and root-only exclusions. Both language suites consume this file.

Reference: [RFC 8785, sections 3.1–3.2.4](https://www.rfc-editor.org/rfc/rfc8785).
