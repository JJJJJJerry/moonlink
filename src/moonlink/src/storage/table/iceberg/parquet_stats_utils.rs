// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Code adapted from iceberg-rust: https://github.com/apache/iceberg-rust

use iceberg::spec::{Datum, PrimitiveType, SchemaRef, Type};
use iceberg::Result as IcebergResult;
use iceberg::{Error, ErrorKind};
use num_bigint::BigInt;
use num_traits::cast::ToPrimitive;
use parquet::file::statistics::Statistics;
use uuid::Uuid;

use std::collections::HashMap;

/// DM(Jerry): Build an iceberg [`Datum`] for a `DECIMAL(p, s)` column from a Parquet column-statistics
/// mantissa.
///
/// Parquet stores decimal min/max as the unscaled two's-complement integer; the column metadata
/// carries `scale` separately. The only public iceberg-rust 0.9.1 API that lets a caller dictate
/// `scale` is [`Datum::decimal_from_str`] — `iceberg::spec::values::decimal_utils` is module-gated
/// `pub(crate) use`, so we must materialise the canonical decimal text representation ourselves
/// (see DM(Jerry) note below for the per-branch shape).
///
/// | mantissa | scale | output     |
/// |----------|-------|------------|
/// | `12345`  | `2`   | `"123.45"` |
/// | `5`      | `2`   | `"0.05"`   |
/// | `-5`     | `3`   | `"-0.005"` |
/// | `0`      | `2`   | `"0.00"`   |
/// | `12345`  | `0`   | `"12345"`  |
///
/// `:0>width$` zero-pad + `unsigned_abs` (total on `i128::MIN`, unlike `i128::abs`).
///
/// TODO(Jerry): switch to a zero-alloc path the day iceberg-rust exposes either
/// `pub fn Datum::new(PrimitiveType, PrimitiveLiteral)` or `pub use values::decimal_utils`.
fn decimal_datum(mantissa: i128, scale: u32) -> IcebergResult<Datum> {
    let abs = mantissa.unsigned_abs();
    let sign = if mantissa.is_negative() { "-" } else { "" };

    let formatted = if scale == 0 {
        format!("{sign}{abs}")
    } else {
        // Pad to at least `scale + 1` digits so the split point is never at index 0 — this is
        // what makes the `|abs| < 10^scale` case (e.g. mantissa=5 scale=3) emit "0.005" instead
        // of ".005".
        let width = scale as usize + 1;
        let padded = format!("{abs:0>width$}");
        let split = padded.len() - scale as usize;
        format!(
            "{sign}{int}.{frac}",
            int = &padded[..split],
            frac = &padded[split..]
        )
    };

    Datum::decimal_from_str(formatted)
}

#[cfg(test)]
mod decimal_datum_tests {
    use super::*;

    fn render(mantissa: i128, scale: u32) -> String {
        format!("{}", decimal_datum(mantissa, scale).expect("valid decimal"))
    }

    #[test]
    fn integer_scale_zero() {
        assert_eq!(render(12345, 0), "12345");
        assert_eq!(render(-12345, 0), "-12345");
        assert_eq!(render(0, 0), "0");
    }

    #[test]
    fn fractional_with_leading_zero_pad() {
        // |mantissa| < 10^scale: must emit "0.<padded>" not ".<padded>"
        assert_eq!(render(5, 3), "0.005");
        assert_eq!(render(-5, 3), "-0.005");
        assert_eq!(render(0, 2), "0.00");
    }

    #[test]
    fn fractional_split_in_middle() {
        assert_eq!(render(12345, 2), "123.45");
        assert_eq!(render(-12345, 2), "-123.45");
    }

    #[test]
    fn mantissa_equals_scale_boundary() {
        // |mantissa| == 10^scale: emit "1.000..." or "0.999..."
        assert_eq!(render(100, 2), "1.00");
        assert_eq!(render(99, 2), "0.99");
    }

    #[test]
    fn negative_38_digit_boundary() {
        // Regression: `i128::abs` overflows on i128::MIN. We use `unsigned_abs` instead so
        // arbitrarily large negative magnitudes — up to the iceberg DECIMAL(38) ceiling —
        // format correctly without panicking on our side.
        let max_neg_38 = -(10_i128.pow(38) - 1); // -(10^38 - 1) = -99…99 (38 nines)
        assert_eq!(
            render(max_neg_38, 0),
            "-99999999999999999999999999999999999999"
        );
    }

    #[test]
    fn large_precision_38_digits() {
        // Iceberg's max DECIMAL precision is 38. Verify we don't truncate.
        let mantissa = 99_999_999_999_999_999_999_999_999_999_999_999_999_i128; // 38 nines
        assert_eq!(
            render(mantissa, 0),
            "99999999999999999999999999999999999999"
        );
        assert_eq!(
            render(mantissa, 2),
            "999999999999999999999999999999999999.99"
        );
    }
}

// ================================
// get_parquet_stat_min_as_datum
// ================================
//
pub(crate) fn get_parquet_stat_min_as_datum(
    primitive_type: &PrimitiveType,
    stats: &Statistics,
) -> IcebergResult<Option<Datum>> {
    Ok(match (primitive_type, stats) {
        (PrimitiveType::Boolean, Statistics::Boolean(stats)) => {
            stats.min_opt().map(|val| Datum::bool(*val))
        }
        (PrimitiveType::Int, Statistics::Int32(stats)) => {
            stats.min_opt().map(|val| Datum::int(*val))
        }
        (PrimitiveType::Date, Statistics::Int32(stats)) => {
            stats.min_opt().map(|val| Datum::date(*val))
        }
        (PrimitiveType::Long, Statistics::Int64(stats)) => {
            stats.min_opt().map(|val| Datum::long(*val))
        }
        (PrimitiveType::Time, Statistics::Int64(stats)) => {
            let Some(val) = stats.min_opt() else {
                return Ok(None);
            };

            Some(Datum::time_micros(*val)?)
        }
        (PrimitiveType::Timestamp, Statistics::Int64(stats)) => {
            stats.min_opt().map(|val| Datum::timestamp_micros(*val))
        }
        (PrimitiveType::Timestamptz, Statistics::Int64(stats)) => {
            stats.min_opt().map(|val| Datum::timestamptz_micros(*val))
        }
        (PrimitiveType::TimestampNs, Statistics::Int64(stats)) => {
            stats.min_opt().map(|val| Datum::timestamp_nanos(*val))
        }
        (PrimitiveType::TimestamptzNs, Statistics::Int64(stats)) => {
            stats.min_opt().map(|val| Datum::timestamptz_nanos(*val))
        }
        (PrimitiveType::Float, Statistics::Float(stats)) => {
            stats.min_opt().map(|val| Datum::float(*val))
        }
        (PrimitiveType::Double, Statistics::Double(stats)) => {
            stats.min_opt().map(|val| Datum::double(*val))
        }
        (PrimitiveType::String, Statistics::ByteArray(stats)) => {
            let Some(val) = stats.min_opt() else {
                return Ok(None);
            };

            Some(Datum::string(val.as_utf8()?))
        }
        (
            PrimitiveType::Decimal {
                precision: _,
                scale,
            },
            Statistics::ByteArray(stats),
        ) => {
            let Some(bytes) = stats.min_bytes_opt() else {
                return Ok(None);
            };
            // TODO(hjiang): Add unit test.
            let value = i128::from_be_bytes(bytes.try_into()?);
            Some(decimal_datum(value, *scale)?)
        }
        (
            PrimitiveType::Decimal {
                precision: _,
                scale,
            },
            Statistics::FixedLenByteArray(stats),
        ) => {
            let Some(bytes) = stats.min_bytes_opt() else {
                return Ok(None);
            };
            let unscaled_value = BigInt::from_signed_bytes_be(bytes);
            // TODO(hjiang): Add unit test.
            let value = unscaled_value.to_i128().ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Can't convert bytes to i128: {bytes:?}"),
                )
            })?;
            Some(decimal_datum(value, *scale)?)
        }
        (
            PrimitiveType::Decimal {
                precision: _,
                scale,
            },
            Statistics::Int32(stats),
        ) => stats.min_opt().map(|val| {
            // TODO(hjiang): Add unit test.
            let value = i128::from(*val);
            decimal_datum(value, *scale).unwrap()
        }),
        (
            PrimitiveType::Decimal {
                precision: _,
                scale,
            },
            Statistics::Int64(stats),
        ) => stats.min_opt().map(|val| {
            // TODO(hjiang): Add unit test.
            let value = i128::from(*val);
            decimal_datum(value, *scale).unwrap()
        }),
        (PrimitiveType::Uuid, Statistics::FixedLenByteArray(stats)) => {
            let Some(bytes) = stats.min_bytes_opt() else {
                return Ok(None);
            };
            if bytes.len() != 16 {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Invalid length of uuid bytes.",
                ));
            }
            Some(Datum::uuid(Uuid::from_bytes(
                bytes[..16].try_into().unwrap(),
            )))
        }
        (PrimitiveType::Fixed(len), Statistics::FixedLenByteArray(stat)) => {
            let Some(bytes) = stat.min_bytes_opt() else {
                return Ok(None);
            };
            if bytes.len() != *len as usize {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Invalid length of fixed bytes.",
                ));
            }
            Some(Datum::fixed(bytes.to_vec()))
        }
        (PrimitiveType::Binary, Statistics::ByteArray(stat)) => {
            return Ok(stat
                .min_bytes_opt()
                .map(|bytes| Datum::binary(bytes.to_vec())));
        }
        _ => {
            return Ok(None);
        }
    })
}

// ================================
// get_parquet_stat_max_as_datum
// ================================
//
pub(crate) fn get_parquet_stat_max_as_datum(
    primitive_type: &PrimitiveType,
    stats: &Statistics,
) -> IcebergResult<Option<Datum>> {
    Ok(match (primitive_type, stats) {
        (PrimitiveType::Boolean, Statistics::Boolean(stats)) => {
            stats.max_opt().map(|val| Datum::bool(*val))
        }
        (PrimitiveType::Int, Statistics::Int32(stats)) => {
            stats.max_opt().map(|val| Datum::int(*val))
        }
        (PrimitiveType::Date, Statistics::Int32(stats)) => {
            stats.max_opt().map(|val| Datum::date(*val))
        }
        (PrimitiveType::Long, Statistics::Int64(stats)) => {
            stats.max_opt().map(|val| Datum::long(*val))
        }
        (PrimitiveType::Time, Statistics::Int64(stats)) => {
            let Some(val) = stats.max_opt() else {
                return Ok(None);
            };

            Some(Datum::time_micros(*val)?)
        }
        (PrimitiveType::Timestamp, Statistics::Int64(stats)) => {
            stats.max_opt().map(|val| Datum::timestamp_micros(*val))
        }
        (PrimitiveType::Timestamptz, Statistics::Int64(stats)) => {
            stats.max_opt().map(|val| Datum::timestamptz_micros(*val))
        }
        (PrimitiveType::TimestampNs, Statistics::Int64(stats)) => {
            stats.max_opt().map(|val| Datum::timestamp_nanos(*val))
        }
        (PrimitiveType::TimestamptzNs, Statistics::Int64(stats)) => {
            stats.max_opt().map(|val| Datum::timestamptz_nanos(*val))
        }
        (PrimitiveType::Float, Statistics::Float(stats)) => {
            stats.max_opt().map(|val| Datum::float(*val))
        }
        (PrimitiveType::Double, Statistics::Double(stats)) => {
            stats.max_opt().map(|val| Datum::double(*val))
        }
        (PrimitiveType::String, Statistics::ByteArray(stats)) => {
            let Some(val) = stats.max_opt() else {
                return Ok(None);
            };

            Some(Datum::string(val.as_utf8()?))
        }
        (
            PrimitiveType::Decimal {
                precision: _,
                scale,
            },
            Statistics::ByteArray(stats),
        ) => {
            let Some(bytes) = stats.max_bytes_opt() else {
                return Ok(None);
            };
            // TODO(hjiang): Add unit test.
            let value = i128::from_be_bytes(bytes.try_into()?);
            Some(decimal_datum(value, *scale)?)
        }
        (
            PrimitiveType::Decimal {
                precision: _,
                scale,
            },
            Statistics::FixedLenByteArray(stats),
        ) => {
            let Some(bytes) = stats.max_bytes_opt() else {
                return Ok(None);
            };
            // TODO(hjiang): Add unit test.
            let unscaled_value = BigInt::from_signed_bytes_be(bytes);
            let value = unscaled_value.to_i128().ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Can't convert bytes to i128: {bytes:?}"),
                )
            })?;
            Some(decimal_datum(value, *scale)?)
        }
        (
            PrimitiveType::Decimal {
                precision: _,
                scale,
            },
            Statistics::Int32(stats),
        ) => stats.max_opt().map(|val| {
            // TODO(hjiang): Add unit test.
            let value = i128::from(*val);
            decimal_datum(value, *scale).unwrap()
        }),
        (
            PrimitiveType::Decimal {
                precision: _,
                scale,
            },
            Statistics::Int64(stats),
        ) => stats.max_opt().map(|val| {
            // TODO(hjiang): Add unit test.
            let value = i128::from(*val);
            decimal_datum(value, *scale).unwrap()
        }),
        (PrimitiveType::Uuid, Statistics::FixedLenByteArray(stats)) => {
            let Some(bytes) = stats.max_bytes_opt() else {
                return Ok(None);
            };
            if bytes.len() != 16 {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Invalid length of uuid bytes.",
                ));
            }
            Some(Datum::uuid(Uuid::from_bytes(
                bytes[..16].try_into().unwrap(),
            )))
        }
        (PrimitiveType::Fixed(len), Statistics::FixedLenByteArray(stat)) => {
            let Some(bytes) = stat.max_bytes_opt() else {
                return Ok(None);
            };
            if bytes.len() != *len as usize {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Invalid length of fixed bytes.",
                ));
            }
            Some(Datum::fixed(bytes.to_vec()))
        }
        (PrimitiveType::Binary, Statistics::ByteArray(stat)) => {
            return Ok(stat
                .max_bytes_opt()
                .map(|bytes| Datum::binary(bytes.to_vec())));
        }
        _ => {
            return Ok(None);
        }
    })
}

// ================================
// MinMaxColAggregator
// ================================
//
// Used to aggregate min and max value of each column.
pub(crate) struct MinMaxColAggregator {
    lower_bounds: HashMap<i32, Datum>,
    upper_bounds: HashMap<i32, Datum>,
    schema: SchemaRef,
}

impl MinMaxColAggregator {
    /// Creates new and empty `MinMaxColAggregator`
    pub(crate) fn new(schema: SchemaRef) -> Self {
        Self {
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            schema,
        }
    }

    // DM(Jerry): file-level lower_bound is the minimum across all row groups in the file.
    // Replace the stored datum only when the incoming row-group min is strictly smaller.
    // The upstream code (introduced by #933 / commit 349f3b6 "Fix parquet stats") flipped the
    // comparison to `*e < datum`, which kept the *largest* row-group min and produced manifest
    // bounds too narrow on the lower side — readers would then prune files that actually
    // contained matching rows. See tests below for a regression that fails against the old code.
    pub(crate) fn update_state_min(&mut self, field_id: i32, datum: Datum) {
        self.lower_bounds
            .entry(field_id)
            .and_modify(|e| {
                if datum < *e {
                    *e = datum.clone()
                }
            })
            .or_insert(datum);
    }

    // DM(Jerry): symmetric to update_state_min — keep the largest row-group max.
    // The pre-#933 upstream code used `*e > datum` here, which kept the *smallest* row-group max
    // and was never fixed by #933 (which only touched the min branch).
    pub(crate) fn update_state_max(&mut self, field_id: i32, datum: Datum) {
        self.upper_bounds
            .entry(field_id)
            .and_modify(|e| {
                if datum > *e {
                    *e = datum.clone()
                }
            })
            .or_insert(datum);
    }

    /// Update statistics
    pub(crate) fn update(&mut self, field_id: i32, value: Statistics) -> IcebergResult<()> {
        let Some(ty) = self
            .schema
            .field_by_id(field_id)
            .map(|f| f.field_type.as_ref())
        else {
            // Following java implementation: https://github.com/apache/iceberg/blob/29a2c456353a6120b8c882ed2ab544975b168d7b/parquet/src/main/java/org/apache/iceberg/parquet/ParquetUtil.java#L163
            // Ignore the field if it is not in schema.
            return Ok(());
        };
        let Type::Primitive(ty) = ty.clone() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("Composed type {ty} is not supported for min max aggregation."),
            ));
        };

        if value.min_is_exact() {
            let Some(min_datum) = get_parquet_stat_min_as_datum(&ty, &value)? else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Statistics {value} is not match with field type {ty}."),
                ));
            };

            self.update_state_min(field_id, min_datum);
        }

        if value.max_is_exact() {
            let Some(max_datum) = get_parquet_stat_max_as_datum(&ty, &value)? else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Statistics {value} is not match with field type {ty}."),
                ));
            };

            self.update_state_max(field_id, max_datum);
        }

        Ok(())
    }

    /// Returns lower and upper bounds
    pub(crate) fn produce(self) -> (HashMap<i32, Datum>, HashMap<i32, Datum>) {
        (self.lower_bounds, self.upper_bounds)
    }
}

#[cfg(test)]
mod min_max_aggregator_tests {
    //! Regression tests for the lower/upper bound aggregator.
    //!
    //! Background: upstream #933 (commit 349f3b6 "Fix parquet stats") flipped the comparison in
    //! `update_state_min` the wrong way, and never corrected `update_state_max`. Result: for a
    //! Parquet file with N >= 2 row groups, the manifest's lower_bound would be the *largest*
    //! row-group min and the upper_bound would be the *smallest* row-group max — a strictly
    //! narrower range than the true [min, max], causing Iceberg readers to wrongly prune files.
    //!
    //! Single-row-group files never trigger the bug (the first call hits `or_insert` and skips the
    //! comparison branch entirely), which is why the regression went unnoticed in mooncake's
    //! existing test suite. Each test below feeds >= 2 row-group bounds for the same field_id.
    use super::*;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type as IcebergType};
    use std::sync::Arc;

    fn empty_aggregator() -> MinMaxColAggregator {
        // update_state_{min,max} never touch the schema; an empty schema is sufficient. We still
        // build a real one so we don't rely on internal field access of the type.
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![Arc::new(NestedField::required(
                /*id=*/ 1,
                "v".to_string(),
                IcebergType::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .expect("test schema builds");
        MinMaxColAggregator::new(Arc::new(schema))
    }

    fn int(v: i32) -> Datum {
        Datum::int(v)
    }

    #[test]
    fn min_keeps_smallest_across_multiple_row_groups() {
        let mut agg = empty_aggregator();
        // Simulated row-group mins for field_id=1: 5, 1, 3. True file min = 1.
        agg.update_state_min(1, int(5));
        agg.update_state_min(1, int(1));
        agg.update_state_min(1, int(3));
        let (lower, _) = agg.produce();
        assert_eq!(
            lower.get(&1),
            Some(&int(1)),
            "lower_bound must be the smallest row-group min (regression: old code kept 5)"
        );
    }

    #[test]
    fn max_keeps_largest_across_multiple_row_groups() {
        let mut agg = empty_aggregator();
        // Simulated row-group maxes for field_id=1: 5, 10, 3. True file max = 10.
        agg.update_state_max(1, int(5));
        agg.update_state_max(1, int(10));
        agg.update_state_max(1, int(3));
        let (_, upper) = agg.produce();
        assert_eq!(
            upper.get(&1),
            Some(&int(10)),
            "upper_bound must be the largest row-group max (regression: old code kept 3)"
        );
    }

    #[test]
    fn ascending_order_is_not_special_cased() {
        // Ordered input: 1, 2, 3. The bug only manifested on certain orderings, so verify both
        // monotonic directions behave correctly.
        let mut agg = empty_aggregator();
        for v in [1, 2, 3] {
            agg.update_state_min(1, int(v));
            agg.update_state_max(1, int(v));
        }
        let (lower, upper) = agg.produce();
        assert_eq!(lower.get(&1), Some(&int(1)));
        assert_eq!(upper.get(&1), Some(&int(3)));
    }

    #[test]
    fn descending_order_is_not_special_cased() {
        let mut agg = empty_aggregator();
        for v in [3, 2, 1] {
            agg.update_state_min(1, int(v));
            agg.update_state_max(1, int(v));
        }
        let (lower, upper) = agg.produce();
        assert_eq!(lower.get(&1), Some(&int(1)));
        assert_eq!(upper.get(&1), Some(&int(3)));
    }

    #[test]
    fn duplicate_values_do_not_corrupt_bounds() {
        let mut agg = empty_aggregator();
        for v in [2, 2, 2] {
            agg.update_state_min(1, int(v));
            agg.update_state_max(1, int(v));
        }
        let (lower, upper) = agg.produce();
        assert_eq!(lower.get(&1), Some(&int(2)));
        assert_eq!(upper.get(&1), Some(&int(2)));
    }

    #[test]
    fn multiple_fields_aggregate_independently() {
        let mut agg = empty_aggregator();
        // field 1: min 1 max 9, field 2: min -5 max 0
        agg.update_state_min(1, int(7));
        agg.update_state_min(1, int(1));
        agg.update_state_max(1, int(7));
        agg.update_state_max(1, int(9));

        agg.update_state_min(2, int(-1));
        agg.update_state_min(2, int(-5));
        agg.update_state_max(2, int(-1));
        agg.update_state_max(2, int(0));

        let (lower, upper) = agg.produce();
        assert_eq!(lower.get(&1), Some(&int(1)));
        assert_eq!(upper.get(&1), Some(&int(9)));
        assert_eq!(lower.get(&2), Some(&int(-5)));
        assert_eq!(upper.get(&2), Some(&int(0)));
    }
}
