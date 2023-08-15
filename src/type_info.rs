use std::{
    cmp::Ordering,
    fmt::{Arguments, Display},
};

use arrayvec::ArrayString;
use serde::{Deserialize, Serialize};
use sqlx_core::type_info::TypeInfo;

/// Information about an Exasol data type.
///
/// Note that the [`DataTypeName`] is automatically constructed
/// from the provided [`ExaDataType`].
#[derive(Debug, Clone, Deserialize)]
#[serde(from = "ExaDataType")]
pub struct ExaTypeInfo {
    name: DataTypeName,
    datatype: ExaDataType,
}

impl ExaTypeInfo {
    /// Checks compatibility with other data types.
    ///
    /// Returns true if the [`self`] instance is compatible/bigger/able to
    /// accommodate the `other` instance.
    pub fn compatible(&self, other: &Self) -> bool {
        self.datatype.compatible(&other.datatype)
    }
}

impl From<ExaDataType> for ExaTypeInfo {
    fn from(datatype: ExaDataType) -> Self {
        let name = datatype.full_name();
        Self { name, datatype }
    }
}

impl Serialize for ExaTypeInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.datatype.serialize(serializer)
    }
}

impl PartialEq for ExaTypeInfo {
    fn eq(&self, other: &Self) -> bool {
        self.datatype == other.datatype
    }
}

impl Display for ExaTypeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl TypeInfo for ExaTypeInfo {
    fn is_null(&self) -> bool {
        false
    }

    /// We're going against `sqlx` here, but knowing the full data type definition
    /// is actually very helpful when displaying error messages, so... ¯\_(ツ)_/¯.
    ///
    /// In fact, error messages seem to be the only place where this is being used,
    /// particularly when trying to decode a value but the data type provided by the
    /// database does not match/fit inside the Rust data type.
    fn name(&self) -> &str {
        self.name.as_ref()
    }
}

/// Datatype definitions enum, as Exasol sees them.
///
/// If you manually construct them, be aware that there is a [`DataTypeName`]
/// automatically constructed when converting to [`ExaTypeInfo`] and there
/// are compatibility checks set in place.
///
/// In case of incompatibility, the definition is displayed for troubleshooting.
///
///
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
#[serde(tag = "type")]
pub enum ExaDataType {
    Null,
    Boolean,
    Char(StringLike),
    Date,
    Decimal(Decimal),
    Double,
    Geometry(Geometry),
    #[serde(rename = "INTERVAL DAY TO SECOND")]
    IntervalDayToSecond(IntervalDayToSecond),
    #[serde(rename = "INTERVAL YEAR TO MONTH")]
    IntervalYearToMonth(IntervalYearToMonth),
    Timestamp,
    #[serde(rename = "TIMESTAMP WITH LOCAL TIME ZONE")]
    TimestampWithLocalTimeZone,
    Varchar(StringLike),
    Hashtype(Hashtype),
}

impl ExaDataType {
    const NULL: &str = "NULL";
    const BOOLEAN: &str = "BOOLEAN";
    const CHAR: &str = "CHAR";
    const DATE: &str = "DATE";
    const DECIMAL: &str = "DECIMAL";
    const DOUBLE: &str = "DOUBLE PRECISION";
    const GEOMETRY: &str = "GEOMETRY";
    const INTERVAL_DAY_TO_SECOND: &str = "INTERVAL DAY TO SECOND";
    const INTERVAL_YEAR_TO_MONTH: &str = "INTERVAL YEAR TO MONTH";
    const TIMESTAMP: &str = "TIMESTAMP";
    const TIMESTAMP_WITH_LOCAL_TIME_ZONE: &str = "TIMESTAMP WITH LOCAL TIME ZONE";
    const VARCHAR: &str = "VARCHAR";
    const HASHTYPE: &str = "HASHTYPE";

    /// Returns `true` if this instance is compatible with the other one provided.
    ///
    /// Compatibility means that the [`self`] instance is bigger/able to accommodate
    /// the other instance.
    pub fn compatible(&self, other: &Self) -> bool {
        match self {
            ExaDataType::Null => true,
            ExaDataType::Boolean => matches!(other, ExaDataType::Boolean | ExaDataType::Null),
            ExaDataType::Char(c) | ExaDataType::Varchar(c) => c.compatible(other),
            ExaDataType::Date => matches!(
                other,
                ExaDataType::Date
                    | ExaDataType::Char(_)
                    | ExaDataType::Varchar(_)
                    | ExaDataType::Null
            ),
            ExaDataType::Decimal(d) => d.compatible(other),
            ExaDataType::Double => match other {
                ExaDataType::Double | ExaDataType::Null => true,
                ExaDataType::Decimal(d) if d.scale > 0 => true,
                _ => false,
            },
            ExaDataType::Geometry(g) => g.compatible(other),
            ExaDataType::IntervalDayToSecond(ids) => ids.compatible(other),
            ExaDataType::IntervalYearToMonth(iym) => iym.compatible(other),
            ExaDataType::Timestamp => matches!(
                other,
                ExaDataType::Timestamp
                    | ExaDataType::TimestampWithLocalTimeZone
                    | ExaDataType::Char(_)
                    | ExaDataType::Varchar(_)
                    | ExaDataType::Null
            ),
            ExaDataType::TimestampWithLocalTimeZone => matches!(
                other,
                ExaDataType::TimestampWithLocalTimeZone
                    | ExaDataType::Timestamp
                    | ExaDataType::Char(_)
                    | ExaDataType::Varchar(_)
                    | ExaDataType::Null
            ),
            ExaDataType::Hashtype(h) => h.compatible(other),
        }
    }

    fn full_name(&self) -> DataTypeName {
        match self {
            ExaDataType::Null => Self::NULL.into(),
            ExaDataType::Boolean => Self::BOOLEAN.into(),
            ExaDataType::Date => Self::DATE.into(),
            ExaDataType::Double => Self::DOUBLE.into(),
            ExaDataType::Timestamp => Self::TIMESTAMP.into(),
            ExaDataType::TimestampWithLocalTimeZone => Self::TIMESTAMP_WITH_LOCAL_TIME_ZONE.into(),
            ExaDataType::Char(c) | ExaDataType::Varchar(c) => {
                format_args!("{}({}) {}", self.as_ref(), c.size, c.character_set).into()
            }
            ExaDataType::Decimal(d) => {
                format_args!("{}({}, {})", self.as_ref(), d.precision, d.scale).into()
            }
            ExaDataType::Geometry(g) => format_args!("{}({})", self.as_ref(), g.srid).into(),
            ExaDataType::IntervalDayToSecond(ids) => format_args!(
                "INTERVAL DAY({}) TO SECOND({})",
                ids.precision, ids.fraction
            )
            .into(),
            ExaDataType::IntervalYearToMonth(iym) => {
                format_args!("INTERVAL YEAR({}) TO MONTH", iym.precision).into()
            }
            ExaDataType::Hashtype(h) => format_args!("{}({} BYTE)", self.as_ref(), h.size()).into(),
        }
    }
}

impl AsRef<str> for ExaDataType {
    fn as_ref(&self) -> &str {
        match self {
            ExaDataType::Null => Self::NULL,
            ExaDataType::Boolean => Self::BOOLEAN,
            ExaDataType::Char(_) => Self::CHAR,
            ExaDataType::Date => Self::DATE,
            ExaDataType::Decimal(_) => Self::DECIMAL,
            ExaDataType::Double => Self::DOUBLE,
            ExaDataType::Geometry(_) => Self::GEOMETRY,
            ExaDataType::IntervalDayToSecond(_) => Self::INTERVAL_DAY_TO_SECOND,
            ExaDataType::IntervalYearToMonth(_) => Self::INTERVAL_YEAR_TO_MONTH,
            ExaDataType::Timestamp => Self::TIMESTAMP,
            ExaDataType::TimestampWithLocalTimeZone => Self::TIMESTAMP_WITH_LOCAL_TIME_ZONE,
            ExaDataType::Varchar(_) => Self::VARCHAR,
            ExaDataType::Hashtype(_) => Self::HASHTYPE,
        }
    }
}

/// A data type's name, composed from an instance of [`ExaDataType`].
/// For performance's sake, since data type names are small,
/// we either store them statically or as inlined strings.
///
/// *IMPORTANT*: Creating absurd [`ExaDataType`] can result in panics
/// if the name exceeds the inlined strings max capacity. Valid values always fit.
#[derive(Debug, Clone)]
enum DataTypeName {
    Static(&'static str),
    Inline(ArrayString<30>),
}

impl AsRef<str> for DataTypeName {
    fn as_ref(&self) -> &str {
        match self {
            DataTypeName::Static(s) => s,
            DataTypeName::Inline(s) => s.as_str(),
        }
    }
}

impl Display for DataTypeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl From<&'static str> for DataTypeName {
    fn from(value: &'static str) -> Self {
        Self::Static(value)
    }
}

impl From<Arguments<'_>> for DataTypeName {
    fn from(value: Arguments<'_>) -> Self {
        Self::Inline(ArrayString::try_from(value).expect("inline data type name too large"))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StringLike {
    size: usize,
    character_set: Charset,
}

impl StringLike {
    pub const MAX_VARCHAR_LEN: usize = 2_000_000;
    pub const MAX_CHAR_LEN: usize = 2000;

    pub fn new(size: usize, character_set: Charset) -> Self {
        Self {
            size,
            character_set,
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn character_set(&self) -> Charset {
        self.character_set
    }

    /// Strings are complex and ensuring one fits inside
    /// a database column would imply a lot of overhead.
    ///
    /// So just let the database do its thing and throw an error.
    pub fn compatible(&self, ty: &ExaDataType) -> bool {
        matches!(
            ty,
            ExaDataType::Char(_)
                | ExaDataType::Varchar(_)
                | ExaDataType::Null
                | ExaDataType::Date
                | ExaDataType::Geometry(_)
                | ExaDataType::Hashtype(_)
                | ExaDataType::IntervalDayToSecond(_)
                | ExaDataType::IntervalYearToMonth(_)
                | ExaDataType::Timestamp
                | ExaDataType::TimestampWithLocalTimeZone
        )
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Charset {
    Utf8,
    Ascii,
}

impl AsRef<str> for Charset {
    fn as_ref(&self) -> &str {
        match self {
            Charset::Utf8 => "UTF8",
            Charset::Ascii => "ASCII",
        }
    }
}

impl Display for Charset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Decimal {
    precision: u32,
    scale: u32,
}

impl Decimal {
    pub const MAX_8BIT_PRECISION: u32 = 3;
    pub const MAX_16BIT_PRECISION: u32 = 5;
    pub const MAX_32BIT_PRECISION: u32 = 10;
    pub const MAX_64BIT_PRECISION: u32 = 20;
    pub const MAX_PRECISION: u32 = 36;
    pub const MAX_SCALE: u32 = 35;

    pub fn new(precision: u32, scale: u32) -> Self {
        Self { precision, scale }
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn scale(&self) -> u32 {
        self.scale
    }

    pub fn compatible(&self, ty: &ExaDataType) -> bool {
        match ty {
            ExaDataType::Decimal(d) => self >= d,
            ExaDataType::Double => self.scale > 0,
            ExaDataType::Null => true,
            _ => false,
        }
    }
}

/// The purpose of this is to be able to tell if some [`Decimal`] fits
/// inside another [`Decimal`].
///
/// Therefore, we consider cases such as:
/// - DECIMAL(10, 1) != DECIMAL(9, 2)
/// - DECIMAL(10, 1) != DECIMAL(10, 2)
/// - DECIMAL(10, 1) < DECIMAL(11, 2)
/// - DECIMAL(10, 1) < DECIMAL(17, 4)
///
/// - DECIMAL(10, 1) > DECIMAL(9, 1)
/// - DECIMAL(10, 1) = DECIMAL(10, 1)
/// - DECIMAL(10, 1) < DECIMAL(11, 1)
///
/// So, our rule will be:
/// - if a.scale > b.scale, a > b if and only if (a.precision - a.scale) >= (b.precision - b.scale)
/// - if a.scale == b.scale, a == b if and only if (a.precision - a.scale) == (b.precision - b.scale)
/// - if a.scale < b.scale, a < b if and only if (a.precision - a.scale) <= (b.precision - b.scale)
impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let self_diff = self.precision - self.scale;
        let other_diff = other.precision - other.scale;

        let scale_cmp = self.scale.partial_cmp(&other.scale);
        let diff_cmp = self_diff.partial_cmp(&other_diff);

        match (scale_cmp, diff_cmp) {
            (Some(Ordering::Greater), Some(Ordering::Greater)) => Some(Ordering::Greater),
            (Some(Ordering::Greater), Some(Ordering::Equal)) => Some(Ordering::Greater),
            (Some(Ordering::Equal), ord) => ord,
            (Some(Ordering::Less), Some(Ordering::Less)) => Some(Ordering::Less),
            (Some(Ordering::Less), Some(Ordering::Equal)) => Some(Ordering::Less),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Geometry {
    srid: u16,
}

impl Geometry {
    pub fn new(srid: u16) -> Self {
        Self { srid }
    }

    pub fn srid(&self) -> u16 {
        self.srid
    }

    pub fn compatible(&self, ty: &ExaDataType) -> bool {
        match ty {
            ExaDataType::Geometry(g) => self.srid == g.srid,
            ExaDataType::Varchar(_) | ExaDataType::Char(_) | ExaDataType::Null => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IntervalDayToSecond {
    precision: u32,
    fraction: u32,
}

impl PartialOrd for IntervalDayToSecond {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let precision_cmp = self.precision.partial_cmp(&other.precision);
        let fraction_cmp = self.fraction.partial_cmp(&other.fraction);

        match (precision_cmp, fraction_cmp) {
            (Some(Ordering::Equal), Some(Ordering::Equal)) => Some(Ordering::Equal),
            (Some(Ordering::Equal), Some(Ordering::Less))
            | (Some(Ordering::Less), Some(Ordering::Less))
            | (Some(Ordering::Less), Some(Ordering::Equal)) => Some(Ordering::Less),
            (Some(Ordering::Equal), Some(Ordering::Greater))
            | (Some(Ordering::Greater), Some(Ordering::Greater))
            | (Some(Ordering::Greater), Some(Ordering::Equal)) => Some(Ordering::Greater),
            _ => None,
        }
    }
}

impl IntervalDayToSecond {
    /// The fraction has the weird behavior of shifting the milliseconds up
    /// value and mixing it with the seconds, minutes, hours or even the days
    /// when the value exceeds 3 (the max milliseconds digits limit).
    ///
    /// See: https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/to_dsinterval.htm?Highlight=fraction%20interval
    ///
    /// Therefore, we'll only be handling fractions smaller or equal to 3, as I don't
    /// even know how to handle values above that
    pub const MAX_SUPPORTED_FRACTION: u32 = 3;
    pub const MAX_PRECISION: u32 = 9;

    pub fn new(precision: u32, fraction: u32) -> Self {
        Self {
            precision,
            fraction,
        }
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn fraction(&self) -> u32 {
        self.fraction
    }

    pub fn compatible(&self, ty: &ExaDataType) -> bool {
        match ty {
            ExaDataType::IntervalDayToSecond(i) => self >= i,
            ExaDataType::Varchar(_) | ExaDataType::Char(_) | ExaDataType::Null => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, PartialOrd)]
#[serde(rename_all = "camelCase")]
pub struct IntervalYearToMonth {
    precision: u32,
}

impl IntervalYearToMonth {
    pub const MAX_PRECISION: u32 = 9;

    pub fn new(precision: u32) -> Self {
        Self { precision }
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn compatible(&self, ty: &ExaDataType) -> bool {
        match ty {
            ExaDataType::IntervalYearToMonth(i) => self >= i,
            ExaDataType::Varchar(_) | ExaDataType::Char(_) | ExaDataType::Null => true,
            _ => false,
        }
    }
}

/// The Exasol `HASHTYPE` data type.
/// Note that Exasol returns the size doubled, as by default the HASHTYPE_FORMAT is HEX.
/// This is handled internally and should not be a concern for any consumer
/// of this type.
///
/// Therefore, for a datatype such as [`uuid::Uuid`] which is `16` bytes long,
/// the `size` field will be `32`, but the [`HashType::new()`] or [`HashType::size()`]
/// will accept/return `16` to make it easier to reason with.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, PartialOrd)]
#[serde(rename_all = "camelCase")]
pub struct Hashtype {
    size: u16,
}

impl Hashtype {
    pub const MAX_HASHTYPE_SIZE: u16 = 1024;

    pub fn new(size: u16) -> Self {
        Self { size: size * 2 }
    }

    pub fn size(&self) -> u16 {
        self.size / 2
    }

    pub fn compatible(&self, ty: &ExaDataType) -> bool {
        match ty {
            ExaDataType::Hashtype(h) => self.size >= h.size,
            ExaDataType::Varchar(_) | ExaDataType::Char(_) | ExaDataType::Null => true,
            _ => false,
        }
    }
}

/// Mainly adding these so that we ensure the inlined type names
/// won't panic when created with their max values.
///
/// If the max values work, the lower ones inherently will too.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_name() {
        let data_type = ExaDataType::Null;
        assert_eq!(data_type.full_name().as_ref(), "NULL");
    }

    #[test]
    fn test_boolean_name() {
        let data_type = ExaDataType::Boolean;
        assert_eq!(data_type.full_name().as_ref(), "BOOLEAN");
    }

    #[test]
    fn test_max_char_name() {
        let string_like = StringLike::new(StringLike::MAX_CHAR_LEN, Charset::Ascii);
        let data_type = ExaDataType::Char(string_like);
        assert_eq!(
            data_type.full_name().as_ref(),
            format!("CHAR({}) ASCII", StringLike::MAX_CHAR_LEN)
        );
    }

    #[test]
    fn test_date_name() {
        let data_type = ExaDataType::Date;
        assert_eq!(data_type.full_name().as_ref(), "DATE");
    }

    #[test]
    fn test_max_decimal_name() {
        let decimal = Decimal::new(Decimal::MAX_PRECISION, Decimal::MAX_SCALE);
        let data_type = ExaDataType::Decimal(decimal);
        assert_eq!(
            data_type.full_name().as_ref(),
            format!(
                "DECIMAL({}, {})",
                Decimal::MAX_PRECISION,
                Decimal::MAX_SCALE
            )
        );
    }

    #[test]
    fn test_double_name() {
        let data_type = ExaDataType::Double;
        assert_eq!(data_type.full_name().as_ref(), "DOUBLE PRECISION");
    }

    #[test]
    fn test_max_geometry_name() {
        let geometry = Geometry::new(u16::MAX);
        let data_type = ExaDataType::Geometry(geometry);
        assert_eq!(
            data_type.full_name().as_ref(),
            format!("GEOMETRY({})", u16::MAX)
        );
    }

    #[test]
    fn test_max_interval_day_name() {
        let ids = IntervalDayToSecond::new(
            IntervalDayToSecond::MAX_PRECISION,
            IntervalDayToSecond::MAX_SUPPORTED_FRACTION,
        );
        let data_type = ExaDataType::IntervalDayToSecond(ids);
        assert_eq!(
            data_type.full_name().as_ref(),
            format!(
                "INTERVAL DAY({}) TO SECOND({})",
                IntervalDayToSecond::MAX_PRECISION,
                IntervalDayToSecond::MAX_SUPPORTED_FRACTION
            )
        );
    }

    #[test]
    fn test_max_interval_year_name() {
        let iym = IntervalYearToMonth::new(IntervalYearToMonth::MAX_PRECISION);
        let data_type = ExaDataType::IntervalYearToMonth(iym);
        assert_eq!(
            data_type.full_name().as_ref(),
            format!(
                "INTERVAL YEAR({}) TO MONTH",
                IntervalYearToMonth::MAX_PRECISION,
            )
        );
    }

    #[test]
    fn test_timestamp_name() {
        let data_type = ExaDataType::Timestamp;
        assert_eq!(data_type.full_name().as_ref(), "TIMESTAMP");
    }

    #[test]
    fn test_timestamp_with_tz_name() {
        let data_type = ExaDataType::TimestampWithLocalTimeZone;
        assert_eq!(
            data_type.full_name().as_ref(),
            "TIMESTAMP WITH LOCAL TIME ZONE"
        );
    }

    #[test]
    fn test_max_varchar_name() {
        let string_like = StringLike::new(StringLike::MAX_VARCHAR_LEN, Charset::Ascii);
        let data_type = ExaDataType::Varchar(string_like);
        assert_eq!(
            data_type.full_name().as_ref(),
            format!("VARCHAR({}) ASCII", StringLike::MAX_VARCHAR_LEN)
        );
    }

    #[test]
    fn test_max_hashtype_name() {
        let hashtype = Hashtype::new(Hashtype::MAX_HASHTYPE_SIZE);
        let data_type = ExaDataType::Hashtype(hashtype);
        assert_eq!(
            data_type.full_name().as_ref(),
            format!("HASHTYPE({} BYTE)", Hashtype::MAX_HASHTYPE_SIZE)
        );
    }
}
