use serde::Deserialize;
use serde_json::Value;
use sqlx_core::decode::Decode;
use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::BoxDynError;
use sqlx_core::types::Type;

use crate::arguments::ExaBuffer;
use crate::database::Exasol;
use crate::type_info::{Decimal, ExaTypeInfo};
use crate::value::ExaValueRef;

/// Numbers below this threshold must be serialized/deserialized as integers.
/// The ones above must be treated as strings.
const MAX_U64_NUMERIC: u64 = 1000000000000000000;

impl Type<Exasol> for u8 {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Decimal(Decimal::new(Decimal::MAX_8BIT_PRECISION, 0))
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl Encode<'_, Exasol> for u8 {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append(self);
        IsNull::No
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        let precision = self.checked_ilog10().unwrap_or_default() + 1;
        Some(ExaTypeInfo::Decimal(Decimal::new(precision, 0)))
    }
}

impl Decode<'_, Exasol> for u8 {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <Self as Deserialize>::deserialize(value.value).map_err(From::from)
    }
}

impl Type<Exasol> for u16 {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Decimal(Decimal::new(Decimal::MAX_16BIT_PRECISION, 0))
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl Encode<'_, Exasol> for u16 {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append(self);
        IsNull::No
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        let precision = self.checked_ilog10().unwrap_or_default() + 1;
        Some(ExaTypeInfo::Decimal(Decimal::new(precision, 0)))
    }
}

impl Decode<'_, Exasol> for u16 {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <Self as Deserialize>::deserialize(value.value).map_err(From::from)
    }
}

impl Type<Exasol> for u32 {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Decimal(Decimal::new(Decimal::MAX_32BIT_PRECISION, 0))
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl Encode<'_, Exasol> for u32 {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append(self);
        IsNull::No
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        let precision = self.checked_ilog10().unwrap_or_default() + 1;
        Some(ExaTypeInfo::Decimal(Decimal::new(precision, 0)))
    }
}

impl Decode<'_, Exasol> for u32 {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <Self as Deserialize>::deserialize(value.value).map_err(From::from)
    }
}

impl Type<Exasol> for u64 {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Decimal(Decimal::new(Decimal::MAX_64BIT_PRECISION, 0))
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl Encode<'_, Exasol> for u64 {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        if self < &MAX_U64_NUMERIC {
            buf.append(self);
        } else {
            // Large numbers get serialized as strings
            buf.append(format_args!("{self}"));
        };

        IsNull::No
    }
}

impl Decode<'_, Exasol> for u64 {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        match value.value {
            Value::Number(n) => <Self as Deserialize>::deserialize(n).map_err(From::from),
            Value::String(s) => serde_json::from_str(s).map_err(From::from),
            v => Err(format!("invalid u64 value: {v}").into()),
        }
    }
}
