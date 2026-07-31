use std::collections::HashSet;
use std::io;

/// Extract uint32 Edition defaults from `RuntimeDefaults` before the Rust
/// build downgrades the canonical schema to prost-compatible proto3.
pub(crate) fn runtime_u32_defaults(input: &str) -> io::Result<Vec<(String, u32)>> {
    let mut defaults = Vec::new();
    let mut names = HashSet::new();
    let mut in_runtime_defaults = false;
    let mut saw_runtime_defaults = false;
    let mut field_name: Option<String> = None;

    for line in input.lines() {
        let trimmed = line.trim();
        if !in_runtime_defaults {
            if trimmed == "message RuntimeDefaults {" {
                in_runtime_defaults = true;
                saw_runtime_defaults = true;
            }
            continue;
        }
        if trimmed == "}" {
            if let Some(name) = field_name {
                return Err(invalid_data(format!(
                    "RuntimeDefaults.{name} is missing a uint32 default"
                )));
            }
            return Ok(defaults);
        }
        if field_name.is_none() && trimmed.starts_with("uint32 ") && trimmed.ends_with('[') {
            let Some((name, _)) = trimmed.trim_start_matches("uint32 ").split_once('=') else {
                return Err(invalid_data("malformed RuntimeDefaults uint32 field"));
            };
            let name = name.trim();
            if name.is_empty() || !names.insert(name.to_owned()) {
                return Err(invalid_data(format!(
                    "invalid or duplicate RuntimeDefaults field {name:?}"
                )));
            }
            field_name = Some(name.to_owned());
            continue;
        }
        if let Some(raw_value) = trimmed.strip_prefix("default = ") {
            let Some(name) = field_name.take() else {
                return Err(invalid_data(
                    "RuntimeDefaults default is not attached to a uint32 field",
                ));
            };
            let value = raw_value.trim_end_matches(',').trim();
            let value = value.parse::<u32>().map_err(|error| {
                invalid_data(format!(
                    "RuntimeDefaults.{name} default {value:?} is not uint32: {error}"
                ))
            })?;
            defaults.push((name, value));
            continue;
        }
        if trimmed.ends_with("];")
            && let Some(name) = field_name.take()
        {
            return Err(invalid_data(format!(
                "RuntimeDefaults.{name} is missing a uint32 default"
            )));
        }
    }

    if saw_runtime_defaults {
        Err(invalid_data("unterminated RuntimeDefaults message"))
    } else {
        Err(invalid_data("RuntimeDefaults message is missing"))
    }
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::runtime_u32_defaults;

    #[test]
    fn extracts_every_uint32_default() {
        let defaults = runtime_u32_defaults(
            r#"
            message RuntimeDefaults {
              uint32 claim_lease_duration_seconds = 1 [
                features.field_presence = EXPLICIT,
                default = 900
              ];
              uint32 maximum_retry_attempts = 2 [
                features.field_presence = EXPLICIT,
                default = 100
              ];
            }
            "#,
        )
        .unwrap();

        assert_eq!(
            defaults,
            vec![
                ("claim_lease_duration_seconds".into(), 900),
                ("maximum_retry_attempts".into(), 100),
            ]
        );
    }

    #[test]
    fn rejects_missing_and_non_uint32_defaults() {
        let cases = [
            (
                "missing",
                r#"
                message RuntimeDefaults {
                  uint32 maximum_retry_attempts = 1 [
                    features.field_presence = EXPLICIT
                  ];
                }
                "#,
                "missing a uint32 default",
            ),
            (
                "non-uint32",
                r#"
                message RuntimeDefaults {
                  uint32 maximum_retry_attempts = 1 [
                    features.field_presence = EXPLICIT,
                    default = nope
                  ];
                }
                "#,
                "is not uint32",
            ),
        ];

        for (name, input, want) in cases {
            let error = runtime_u32_defaults(input).unwrap_err();
            assert!(
                error.to_string().contains(want),
                "{name}: error {error:?} does not contain {want:?}",
            );
        }
    }
}
