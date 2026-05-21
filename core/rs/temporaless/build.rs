//! Proto codegen for the Rust SDK.
//!
//! The canonical proto file (`api/temporaless/v1/temporaless.proto`) is on
//! edition 2023 so the Go/Python SDKs can pull string defaults from
//! `ReservedNames` via field-level `[default = "..."]`. As of 2026, neither
//! `prost-build` nor `protox` parses edition 2023 syntax yet, so we
//! preprocess the canonical proto into a proto3-equivalent for Rust
//! codegen only — same on-wire bytes, same field numbers, same RPCs.
//!
//! The reserved-name defaults that the editions version expresses via
//! `[default = "..."]` are emitted as Rust constants in a small generated
//! `reserved_names.rs` so the SDK still exposes them as a single source of
//! truth tied to the canonical proto.

use std::fs;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let api_dir = manifest_dir.join("..").join("..").join("..").join("api");
    let canonical_proto = api_dir
        .join("temporaless")
        .join("v1")
        .join("temporaless.proto");
    println!("cargo:rerun-if-changed={}", canonical_proto.display());

    let canonical = fs::read_to_string(&canonical_proto)?;
    let (proto3_text, reserved_defaults) = downgrade_editions_to_proto3(&canonical);

    // Write the proto3-flavored file into OUT_DIR with the same path
    // structure (so the relative import-from-buf.build paths the
    // canonical file uses still resolve).
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let proto_out_root = out_dir.join("proto_src");
    let proto_out_v1 = proto_out_root.join("temporaless").join("v1");
    fs::create_dir_all(&proto_out_v1)?;
    let proto_out_file = proto_out_v1.join("temporaless.proto");
    fs::write(&proto_out_file, &proto3_text)?;

    // Emit Rust constants for the reserved-name defaults.
    let mut reserved_rs = String::from(
        "//! Auto-generated from `ReservedNames` field defaults in the\n\
         //! canonical proto. Single source of truth — do not edit.\n\n",
    );
    for (name, value) in &reserved_defaults {
        reserved_rs.push_str(&format!(
            "pub const {}: &str = {value:?};\n",
            name.to_uppercase()
        ));
    }
    fs::write(out_dir.join("reserved_names.rs"), reserved_rs)?;

    // protox parses (pure-Rust, no protoc); prost-build compiles to Rust.
    let descriptor_set = protox::Compiler::new([proto_out_root.as_path()])?
        .include_imports(true)
        .include_source_info(true)
        .open_files([proto_out_file.strip_prefix(&proto_out_root)?.to_path_buf()])?
        .file_descriptor_set();

    prost_build::Config::new().compile_fds(descriptor_set)?;

    Ok(())
}

/// Convert an edition-2023 proto file into a proto3-equivalent for Rust
/// codegen. Returns the rewritten source and the list of `ReservedNames`
/// defaults so the SDK can re-expose them as Rust constants.
///
/// Transformations:
///   * `edition = "2023";` → `syntax = "proto3";`
///   * `option features.field_presence = IMPLICIT;` → deleted
///   * Within `message ReservedNames { ... }`, multi-line fields that carry
///     `[features.field_presence = EXPLICIT, default = "..."]` collapse to
///     bare `string foo = N;` and the default is exported as a Rust const.
///   * `import "buf/validate/validate.proto";` → deleted (Rust SDK doesn't
///     run protovalidate — validation happens in Go/Python at write time).
///   * Multi-line and inline `(buf.validate.field)` / `(buf.validate.message)`
///     option blocks → stripped.
fn downgrade_editions_to_proto3(input: &str) -> (String, Vec<(String, String)>) {
    let mut output = String::with_capacity(input.len());
    let mut reserved_defaults: Vec<(String, String)> = Vec::new();

    // Track whether we're inside `message ReservedNames { ... }`.
    let mut in_reserved_names = false;
    let mut brace_depth = 0i32; // depth INSIDE ReservedNames only

    let lines: Vec<&str> = input.lines().collect();
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i];

        // Edition / file-level option lines.
        if line.trim_start().starts_with("edition = ") {
            output.push_str("syntax = \"proto3\";\n");
            i += 1;
            continue;
        }
        if line.trim().starts_with("option features.field_presence") {
            i += 1;
            continue;
        }

        // Protovalidate import — not used by the Rust SDK; validation
        // happens in Go/Python at write time.
        if line
            .trim()
            .starts_with("import \"buf/validate/validate.proto\"")
        {
            i += 1;
            continue;
        }

        // Strip multi-line / inline protovalidate option blocks that appear
        // inside a field's `[ ... ]` annotation. The canonical proto uses
        // patterns like:
        //   string workflow_id = 1 [
        //     (buf.validate.field).string = { ... },
        //     (buf.validate.field).cel = { ... }
        //   ];
        // and message-level:
        //   option (buf.validate.message).cel = { ... };
        if line.trim().starts_with("option (buf.validate.message)") {
            // Skip until line ending with `};`.
            while i < lines.len() && !lines[i].trim().ends_with("};") {
                i += 1;
            }
            i += 1; // consume the closing line
            continue;
        }

        // Field declarations with a `[` block whose ONLY contents are
        // protovalidate options. Detect via lookahead.
        let trimmed = line.trim_start();
        if (trimmed.starts_with("string ") || trimmed.starts_with("uint32 "))
            && trimmed.trim_end().ends_with('[')
        {
            // Peek ahead: is this a protovalidate-only block (and NOT a
            // ReservedNames default block)?
            let mut end = i;
            let mut has_validate = false;
            let mut has_default = false;
            while end < lines.len() {
                let t = lines[end].trim();
                if t.contains("(buf.validate.") {
                    has_validate = true;
                }
                if t.starts_with("default = ") {
                    has_default = true;
                }
                if t.ends_with("];") {
                    break;
                }
                end += 1;
            }
            if has_validate && !has_default && end < lines.len() {
                // Strip the block; emit only `<type> <name> = N;`.
                let (ty, rest) = trimmed.split_once(' ').unwrap();
                let (name, number_with_bracket) = rest.split_once('=').unwrap();
                let name = name.trim();
                let number = number_with_bracket.trim().trim_end_matches('[').trim();
                let indent: String = line.chars().take_while(|c| c.is_whitespace()).collect();
                output.push_str(&format!("{indent}{ty} {name} = {number};\n"));
                i = end + 1;
                continue;
            }
        }

        // Detect entry into `message ReservedNames`.
        if !in_reserved_names && line.trim_start().starts_with("message ReservedNames") {
            in_reserved_names = true;
            brace_depth = 0;
            // Fall through to emit the message header verbatim.
        }

        if in_reserved_names {
            // Count braces to know when we leave the ReservedNames block.
            for ch in line.chars() {
                if ch == '{' {
                    brace_depth += 1;
                } else if ch == '}' {
                    brace_depth -= 1;
                }
            }

            // Inside ReservedNames, try to rewrite multi-line default fields.
            let trimmed = line.trim_start();
            if trimmed.starts_with("string ") && trimmed.trim_end().ends_with('[') {
                // Peek ahead to confirm this block carries `default = ` and
                // a closing `];` before we consume it.
                let mut end = i + 1;
                let mut default_value: Option<String> = None;
                while end < lines.len() {
                    let t = lines[end].trim();
                    if t.starts_with("default = ") {
                        let v = t
                            .trim_start_matches("default = ")
                            .trim()
                            .trim_end_matches(',');
                        default_value = Some(v.trim_matches('"').to_string());
                    }
                    if t.ends_with("];") {
                        break;
                    }
                    end += 1;
                }
                if let (Some(default), true) = (default_value, end < lines.len()) {
                    // Parse `string <name> = N [`.
                    let after_type = trimmed.strip_prefix("string ").unwrap();
                    let (name, number_with_bracket) = after_type.split_once('=').unwrap();
                    let name = name.trim().to_string();
                    let number = number_with_bracket
                        .trim()
                        .trim_end_matches('[')
                        .trim()
                        .to_string();
                    let indent: String = line.chars().take_while(|c| c.is_whitespace()).collect();
                    output.push_str(&format!("{indent}string {name} = {number};\n"));
                    reserved_defaults.push((name, default));
                    i = end + 1;
                    if brace_depth <= 0 {
                        in_reserved_names = false;
                    }
                    continue;
                }
            }

            if brace_depth <= 0 {
                in_reserved_names = false;
            }
        }

        output.push_str(line);
        output.push('\n');
        i += 1;
    }

    (output, reserved_defaults)
}
