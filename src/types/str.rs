use std::borrow::Cow;

use serde::Deserialize;
use serde_json::{json, Value};
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

use crate::{database::Exasol, type_info::ExaTypeInfo, value::ExaValueRef};

impl Type<Exasol> for str {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Varchar(Default::default())
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(
            ty,
            ExaTypeInfo::Varchar(_) | ExaTypeInfo::Char(_) | ExaTypeInfo::Geometry(_)
        )
    }
}

impl Encode<'_, Exasol> for &'_ str {
    fn encode_by_ref(&self, buf: &mut Vec<[Value; 1]>) -> IsNull {
        if self.is_empty() {
            buf.push([Value::Null]);
            IsNull::Yes
        } else {
            buf.push([json!(self)]);
            IsNull::No
        }
    }
}

impl<'r> Decode<'r, Exasol> for &'r str {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        <&str>::deserialize(value.value).map_err(From::from)
    }
}

impl Type<Exasol> for String {
    fn type_info() -> ExaTypeInfo {
        <str as Type<Exasol>>::type_info()
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        <str as Type<Exasol>>::compatible(ty)
    }
}

impl Encode<'_, Exasol> for String {
    fn encode_by_ref(&self, buf: &mut Vec<[Value; 1]>) -> IsNull {
        <&str as Encode<Exasol>>::encode(&**self, buf)
    }
}

impl Decode<'_, Exasol> for String {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <&str as Decode<Exasol>>::decode(value).map(ToOwned::to_owned)
    }
}

impl Type<Exasol> for Cow<'_, str> {
    fn type_info() -> ExaTypeInfo {
        <&str as Type<Exasol>>::type_info()
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        <&str as Type<Exasol>>::compatible(ty)
    }
}

impl Encode<'_, Exasol> for Cow<'_, str> {
    fn encode_by_ref(&self, buf: &mut Vec<[Value; 1]>) -> IsNull {
        match self {
            Cow::Borrowed(str) => <&str as Encode<Exasol>>::encode(*str, buf),
            Cow::Owned(str) => <&str as Encode<Exasol>>::encode(&**str, buf),
        }
    }
}

impl<'r> Decode<'r, Exasol> for Cow<'r, str> {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        Cow::deserialize(value.value).map_err(From::from)
    }
}
