use std::collections::HashSet;

use exasol::ExaIter;

use macros::test_type_array;
use macros::test_type_invalid;
use macros::test_type_valid;

const MAX_U64_NUMERIC: u64 = 1000000000000000000;
const MIN_I64_NUMERIC: i64 = -999999999999999999;
const MAX_I64_NUMERIC: i64 = 1000000000000000000;

// Bool
test_type_valid!(bool::"BOOLEAN"::(false, true));
test_type_valid!(bool_option<Option<bool>>::"BOOLEAN"::("NULL" => None::<bool>, "true" => Some(true)));
test_type_array!(bool_array<bool>::"BOOLEAN"::(vec![true, false], Vec::<bool>::new(), Some(vec![true, false]), [false; 4], &[false; 4], vec![true, false].into_boxed_slice(), ExaIter::from(HashSet::from([true, false, true]).iter())));
test_type_array!(bool_array_option<Option<bool>>::"BOOLEAN"::(vec![Some(true), Some(false), None]));

// Unsigned integers
test_type_valid!(u8::"DECIMAL(3, 0)"::(u8::MIN, u8::MAX));
test_type_valid!(u16::"DECIMAL(5, 0)"::(u16::MIN, u16::MAX, u16::from(u8::MAX)));
test_type_valid!(u8_in_u16<u16>::"DECIMAL(5, 0)"::(u8::MIN => u16::from(u8::MIN), u8::MAX => u16::from(u8::MAX)));
test_type_valid!(u32::"DECIMAL(10, 0)"::(u32::MIN, u32::MAX, u32::from(u8::MAX), u32::from(u16::MAX)));
test_type_valid!(u8_in_u32<u32>::"DECIMAL(10, 0)"::(u8::MIN => u32::from(u8::MIN), u8::MAX => u32::from(u8::MAX)));
test_type_valid!(u16_in_u32<u32>::"DECIMAL(10, 0)"::(u16::MIN => u32::from(u16::MIN), u16::MAX => u32::from(u16::MAX)));
test_type_valid!(u64::"DECIMAL(20, 0)"::(u64::MIN, u64::MAX, u64::from(u8::MAX), u64::from(u16::MAX), u64::from(u32::MAX), MAX_U64_NUMERIC, MAX_U64_NUMERIC - 1));
test_type_valid!(u8_in_u64<u64>::"DECIMAL(20, 0)"::(u8::MIN => u64::from(u8::MIN), u8::MAX => u64::from(u8::MAX)));
test_type_valid!(u16_in_u64<u64>::"DECIMAL(20, 0)"::(u16::MIN => u64::from(u16::MIN), u16::MAX => u64::from(u16::MAX)));
test_type_valid!(u32_in_u64<u64>::"DECIMAL(20, 0)"::(u32::MIN => u64::from(u32::MIN), u32::MAX => u64::from(u32::MAX)));
test_type_valid!(u64_option<Option<u64>>::"DECIMAL(20, 0)"::("NULL" => None::<u64>, u64::MAX => Some(u64::MAX)));
test_type_array!(u64_array<u64>::"DECIMAL(20, 0)"::(vec![u64::MIN, u64::MAX, 1234567]));

// Signed integers
test_type_valid!(i8::"DECIMAL(3, 0)"::(i8::MIN, i8::MAX));
test_type_valid!(i16::"DECIMAL(5, 0)"::(i16::MIN, i16::MAX, i16::from(i8::MIN), i16::from(i8::MAX)));
test_type_valid!(i8_in_i16<i16>::"DECIMAL(5, 0)"::(i8::MIN => i16::from(i8::MIN), i8::MAX => i16::from(i8::MAX)));
test_type_valid!(i32::"DECIMAL(10, 0)"::(i32::MIN, i32::MAX, i32::from(i8::MIN), i32::from(i8::MAX), i32::from(i16::MIN), i32::from(i16::MAX)));
test_type_valid!(i8_in_i32<i32>::"DECIMAL(10, 0)"::(i8::MIN => i32::from(i8::MIN), i8::MAX => i32::from(i8::MAX)));
test_type_valid!(i16_in_i32<i32>::"DECIMAL(10, 0)"::(i16::MIN => i32::from(i16::MIN), i16::MAX => i32::from(i16::MAX)));
test_type_valid!(i64::"DECIMAL(20, 0)"::(i64::MIN, i64::MAX, i64::from(i8::MIN), i64::from(i8::MAX), i64::from(i16::MIN), i64::from(i16::MAX), i64::from(i32::MIN), i64::from(i32::MAX), MIN_I64_NUMERIC, MIN_I64_NUMERIC - 1, MAX_I64_NUMERIC, MAX_I64_NUMERIC - 1));
test_type_valid!(i8_in_i64<i64>::"DECIMAL(20, 0)"::(i8::MIN => i64::from(i8::MIN), i8::MAX => i64::from(i8::MAX)));
test_type_valid!(i16_in_i64<i64>::"DECIMAL(20, 0)"::(i16::MIN => i64::from(i16::MIN), i16::MAX => i64::from(i16::MAX)));
test_type_valid!(i32_in_i64<i64>::"DECIMAL(20, 0)"::(i32::MIN => i64::from(i32::MIN), i32::MAX => i64::from(i32::MAX)));
test_type_valid!(i64_option<Option<i64>>::"DECIMAL(20, 0)"::("NULL" => None::<i64>, i64::MAX => Some(i64::MAX)));
test_type_array!(i64_array<i64>::"DECIMAL(20, 0)"::(vec![i64::MIN, i64::MAX, 1234567]));

// Floats (and their weirdness)
test_type_valid!(f32::"DOUBLE PRECISION"::(f32::MIN, f32::MAX));
test_type_valid!(f64::"DOUBLE PRECISION"::(-3.40282346638529e38f64, 3.40282346638529e38f64));
test_type_valid!(f32_decimal<f32>::"DECIMAL(36, 16)"::(-1005.0456, 1005.0456, -7462.0, 7462.0));
test_type_valid!(f64_decimal<f64>::"DECIMAL(36, 16)"::(-1005213.0456543, 1005213.0456543, -1005.0456, 1005.0456, -7462.0, 7462.0));
test_type_valid!(f64_option<Option<f64>>::"DOUBLE PRECISION"::("NULL" => None::<f64>, -1005213.0456543 => Some(-1005213.0456543)));
test_type_valid!(f64_decimal_option<Option<f64>>::"DECIMAL(36, 16)"::("NULL" => None::<f64>, -1005213.0456543 => Some(-1005213.0456543)));
test_type_array!(f64_array<f64>::"DOUBLE PRECISION"::(vec![-1005213.0456543, 1005213.0456543, -1005.0456, 1005.0456, -7462.0, 7462.0]));
test_type_array!(f64_decimal_array<f64>::"DECIMAL(36, 16)"::(vec![-1005213.0456543, 1005213.0456543, -1005.0456, 1005.0456, -7462.0, 7462.0]));

// Strings
test_type_valid!(varchar_ascii<String>::"VARCHAR(100) ASCII"::("'first value'" => "first value", "'second value'" => "second value"));
test_type_valid!(varchar_utf8<String>::"VARCHAR(100) UTF8"::("'first value 🦀'" => "first value 🦀", "'second value 🦀'" => "second value 🦀"));
test_type_valid!(char_ascii<String>::"CHAR(10) ASCII"::("'first     '" => "first     ", "'second'" => "second    "));
test_type_valid!(char_utf8<String>::"CHAR(10) UTF8"::("'first 🦀   '" => "first 🦀   ", "'second 🦀'" => "second 🦀  "));
test_type_valid!(varchar_option<Option<String>>::"VARCHAR(10) UTF8"::("''" => None::<String>, "NULL" => None::<String>, "'value'" => Some("value".to_owned())));
test_type_valid!(char_option<Option<String>>::"CHAR(10) UTF8"::("''" => None::<String>, "NULL" => None::<String>, "'value'" => Some("value     ".to_owned())));
test_type_array!(varchar_array<String>::"VARCHAR(10) UTF8"::(vec!["abc".to_string(), "cde".to_string()]));
test_type_array!(char_array<String>::"CHAR(10) UTF8"::(vec!["abc".to_string(), "cde".to_string()]));

// Geometry
test_type_valid!(geometry<String>::"GEOMETRY"::("'POINT (1 2)'" => "POINT (1 2)", "'POINT (3 4)'" => "POINT (3 4)"));
test_type_valid!(geometry_option<Option<String>>::"GEOMETRY"::("''" => None::<String>, "NULL" => None::<String>, "'POINT (3 4)'" => Some("POINT (3 4)".to_owned())));
test_type_array!(geometry_array<String>::"GEOMETRY"::(vec!["POINT (1 2)".to_owned(), "POINT (3 4)".to_owned()]));

#[cfg(feature = "rust_decimal")]
mod rust_decimal_tests {
    use super::*;
    use rust_decimal::Decimal;

    test_type_valid!(rust_decimal_i64<Decimal>::"DECIMAL(36, 16)"::(Decimal::new(i64::MIN, 16), Decimal::new(i64::MAX, 16), Decimal::new(i64::MAX, 10), Decimal::new(i64::MAX, 5), Decimal::new(i64::MAX, 0)));
    test_type_valid!(rust_decimal_i16<Decimal>::"DECIMAL(36, 16)"::(Decimal::new(i64::from(i16::MIN), 5), Decimal::new(i64::from(i16::MAX), 5), Decimal::new(i64::from(i16::MIN), 0), Decimal::new(i64::from(i16::MAX), 0)));
    test_type_valid!(rust_decimal_no_scale<Decimal>::"DECIMAL(36, 0)"::(Decimal::new(-340282346638529, 0), Decimal::new(340282346638529, 0), Decimal::new(0, 0)));
    test_type_valid!(rust_decimal_option<Option<Decimal>>::"DECIMAL(36, 16)"::("NULL" => None::<Decimal>, Decimal::new(i64::from(i16::MIN), 5) => Some(Decimal::new(i64::from(i16::MIN), 5))));
    test_type_array!(rust_decimal_array<Decimal>::"DECIMAL(36, 16)"::(vec![Decimal::new(i64::MIN, 16), Decimal::new(i64::MAX, 16), Decimal::new(i64::MAX, 10), Decimal::new(i64::MAX, 5), Decimal::new(i64::MAX, 0)]));

    test_type_valid!(u8_decimal_scale<Decimal>::"DECIMAL(5, 2)"::(u8::MAX => Decimal::from(u8::MAX)));
    test_type_valid!(u16_decimal_scale<Decimal>::"DECIMAL(7, 2)"::(u16::MAX => Decimal::from(u16::MAX)));
    test_type_valid!(u32_decimal_scale<Decimal>::"DECIMAL(12, 2)"::(u32::MAX => Decimal::from(u32::MAX)));
    test_type_valid!(u64_decimal_scale<Decimal>::"DECIMAL(22, 2)"::(u64::MAX => Decimal::from(u64::MAX)));

    test_type_valid!(i8_decimal_scale<Decimal>::"DECIMAL(5, 2)"::(i8::MIN => Decimal::from(i8::MIN), i8::MAX => Decimal::from(i8::MAX)));
    test_type_valid!(i16_decimal_scale<Decimal>::"DECIMAL(7, 2)"::(i16::MIN => Decimal::from(i16::MIN), i16::MAX => Decimal::from(i16::MAX)));
    test_type_valid!(i32_decimal_scale<Decimal>::"DECIMAL(12, 2)"::(i32::MIN => Decimal::from(i32::MIN), i32::MAX => Decimal::from(i32::MAX)));
    test_type_valid!(i64_decimal_scale<Decimal>::"DECIMAL(22, 2)"::(i64::MIN => Decimal::from(i64::MIN), i64::MAX => Decimal::from(i64::MAX)));
}

#[cfg(feature = "uuid")]
mod uuid_tests {
    use super::*;
    use uuid::Uuid;

    test_type_valid!(uuid<Uuid>::"HASHTYPE(16 BYTE)"::(format!("'{}'", Uuid::from_u64_pair(12345789, 12345789)) => Uuid::from_u64_pair(12345789, 12345789)));
    test_type_valid!(uuid_str<String>::"HASHTYPE(16 BYTE)"::("'a1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8'" => "a1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8", "'a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8'" => "a1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8"));
    test_type_valid!(uuid_option<Option<Uuid>>::"HASHTYPE(16 BYTE)"::("NULL" => None::<Uuid>, "''" => None::<Uuid>, format!("'{}'", Uuid::from_u64_pair(12345789, 12345789)) => Some(Uuid::from_u64_pair(12345789, 12345789))));
    test_type_array!(uuid_array<Uuid>::"HASHTYPE(16 BYTE)"::(vec![Uuid::from_u64_pair(12345789, 12345789), Uuid::from_u64_pair(12345789, 12345789), Uuid::from_u64_pair(12345789, 12345789)]));
}

#[cfg(feature = "chrono")]
mod chrono_tests {
    use super::*;
    use chrono::{DateTime, Duration, Local, NaiveDate, NaiveDateTime, Utc};
    use exasol::Months;

    const TIMESTAMP_FMT: &str = "%Y-%m-%d %H:%M:%S%.6f";
    const DATE_FMT: &str = "%Y-%m-%d";

    test_type_valid!(naive_datetime<NaiveDateTime>::"TIMESTAMP"::("'2023-08-12 19:22:36.591000'" => NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap()));
    test_type_valid!(naive_datetime_str<String>::"TIMESTAMP"::("'2023-08-12 19:22:36.591000'" => "2023-08-12 19:22:36.591000"));
    test_type_valid!(naive_datetime_optional<Option<NaiveDateTime>>::"TIMESTAMP"::("NULL" => None::<NaiveDateTime>, "''" => None::<NaiveDateTime>, "'2023-08-12 19:22:36.591000'" => Some(NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap())));
    test_type_array!(naive_datetime_array<NaiveDateTime>::"TIMESTAMP"::(vec!["2023-08-12 19:22:36.591000", "2023-08-12 19:22:36.591000", "2023-08-12 19:22:36.591000"]));

    test_type_valid!(naive_date<NaiveDate>::"DATE"::("'2023-08-12'" => NaiveDate::parse_from_str("2023-08-12", DATE_FMT).unwrap()));
    test_type_valid!(naive_date_str<String>::"DATE"::("'2023-08-12'" => "2023-08-12"));
    test_type_valid!(naive_date_option<Option<NaiveDate>>::"DATE"::("NULL" => None::<NaiveDate>, "''" => None::<NaiveDate>, "'2023-08-12'" => Some(NaiveDate::parse_from_str("2023-08-12", DATE_FMT).unwrap())));
    test_type_array!(naive_date_array<NaiveDate>::"DATE"::(vec!["2023-08-12", "2023-08-12", "2023-08-12"]));

    test_type_valid!(datetime_utc<DateTime<Utc>>::"TIMESTAMP"::("'2023-08-12 19:22:36.591000'" => NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().and_utc()));
    test_type_valid!(datetime_utc_str<String>::"TIMESTAMP"::("'2023-08-12 19:22:36.591000'" => "2023-08-12 19:22:36.591000"));
    test_type_valid!(datetime_utc_option<Option<DateTime<Utc>>>::"TIMESTAMP"::("NULL" => None::<DateTime<Utc>>, "''" => None::<DateTime<Utc>>, "'2023-08-12 19:22:36.591000'" => Some(NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().and_utc())));
    test_type_array!(datetime_utc_array<DateTime<Utc>>::"TIMESTAMP"::(vec!["2023-08-12 19:22:36.591000", "2023-08-12 19:22:36.591000", "2023-08-12 19:22:36.591000"]));

    test_type_valid!(datetime_local<DateTime<Local>>::"TIMESTAMP WITH LOCAL TIME ZONE"::("'2023-08-12 19:22:36.591000'" => NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().and_local_timezone(Local).unwrap()));
    test_type_valid!(datetime_local_str<String>::"TIMESTAMP WITH LOCAL TIME ZONE"::("'2023-08-12 19:22:36.591000'" => "2023-08-12 19:22:36.591000"));
    test_type_valid!(datetime_local_option<Option<DateTime<Local>>>::"TIMESTAMP WITH LOCAL TIME ZONE"::("NULL" => None::<DateTime<Local>>, "''" => None::<DateTime<Local>>, "'2023-08-12 19:22:36.591000'" => Some(NaiveDateTime::parse_from_str("2023-08-12 19:22:36.591000", TIMESTAMP_FMT).unwrap().and_local_timezone(Local).unwrap())));
    test_type_array!(datetime_local_array<DateTime<Local>>::"TIMESTAMP WITH LOCAL TIME ZONE"::(vec!["2023-08-12 19:22:36.591000", "2023-08-12 19:22:36.591000", "2023-08-12 19:22:36.591000"]));

    test_type_valid!(duration<Duration>::"INTERVAL DAY TO SECOND"::("'10 20:45:50.123'" => Duration::milliseconds(938750123), "'-10 20:45:50.123'" => Duration::milliseconds(-938750123)));
    test_type_valid!(duration_str<String>::"INTERVAL DAY TO SECOND"::("'10 20:45:50.123'" => "+10 20:45:50.123"));
    test_type_valid!(duration_with_prec<Duration>::"INTERVAL DAY(4) TO SECOND"::("'10 20:45:50.123'" => Duration::milliseconds(938750123), "'-10 20:45:50.123'" => Duration::milliseconds(-938750123)));
    test_type_valid!(duration_option<Option<Duration>>::"INTERVAL DAY TO SECOND"::("NULL" => None::<Duration>, "''" => None::<Duration>, "'10 20:45:50.123'" => Some(Duration::milliseconds(938750123))));
    test_type_array!(duration_array<Duration>::"INTERVAL DAY TO SECOND"::(vec!["10 20:45:50.123", "10 20:45:50.123", "10 20:45:50.123"]));

    test_type_valid!(months<Months>::"INTERVAL YEAR TO MONTH"::("'1-5'" => Months::new(17), "'-1-5'" => Months::new(-17)));
    test_type_valid!(months_str<String>::"INTERVAL YEAR TO MONTH"::("'1-5'" => "+01-05"));
    test_type_valid!(months_with_prec<Months>::"INTERVAL YEAR(4) TO MONTH"::("'1000-5'" => Months::new(12005), "'-1000-5'" => Months::new(-12005)));
    test_type_valid!(months_option<Option<Months>>::"INTERVAL YEAR TO MONTH"::("NULL" => None::<Months>, "''" => None::<Months>, "'1-5'" => Some(Months::new(17))));
    test_type_array!(months_array<Months>::"INTERVAL YEAR TO MONTH"::(vec!["1-5", "1-5", "1-5"]));
}

// Test incompatible types
test_type_invalid!(u16_into_u8<u16>::"DECIMAL(3,0)"::(u16::MAX));
test_type_invalid!(u32_into_u8<u32>::"DECIMAL(3,0)"::(u32::MAX));
test_type_invalid!(u32_into_u16<u32>::"DECIMAL(5,0)"::(u32::MAX));
test_type_invalid!(u64_into_u8<u64>::"DECIMAL(3,0)"::(u64::MAX));
test_type_invalid!(u64_into_u16<u64>::"DECIMAL(5,0)"::(u64::MAX));
test_type_invalid!(u64_into_u32<u64>::"DECIMAL(10,0)"::(u64::MAX));

test_type_invalid!(i16_into_i8<i16>::"DECIMAL(3,0)"::(i16::MAX));
test_type_invalid!(i32_into_i8<i32>::"DECIMAL(3,0)"::(i32::MAX));
test_type_invalid!(i32_into_i16<i32>::"DECIMAL(5,0)"::(i32::MAX));
test_type_invalid!(i64_into_i8<i64>::"DECIMAL(3,0)"::(i64::MAX));
test_type_invalid!(i64_into_i16<i64>::"DECIMAL(5,0)"::(i64::MAX));
test_type_invalid!(i64_into_i32<i64>::"DECIMAL(10,0)"::(i64::MAX));

// Not enough room
test_type_invalid!(u16_no_room<u16>::"DECIMAL(5,2)"::(u16::MAX));
test_type_invalid!(u32_no_room<u32>::"DECIMAL(10,2)"::(u32::MAX));
test_type_invalid!(u64_no_room<u64>::"DECIMAL(20,2)"::(u64::MAX));
test_type_invalid!(i16_no_room<i16>::"DECIMAL(5,2)"::(i16::MAX));
test_type_invalid!(i32_no_room<i32>::"DECIMAL(10,2)"::(i32::MAX));
test_type_invalid!(i64_no_room<i64>::"DECIMAL(20,2)"::(i64::MAX));

mod macros {
    macro_rules! test_type_valid {
    ($name:ident<$ty:ty>::$datatype:literal::($($unprepared:expr => $prepared:expr),+)) => {
        paste::item! {
            #[sqlx::test]
            async fn [< test_type_valid_ $name >] (
                mut con: sqlx_core::pool::PoolConnection<exasol::Exasol>,
            ) -> Result<(), sqlx_core::error::BoxDynError> {
                use sqlx_core::{executor::Executor, query::query, query_scalar::query_scalar};

                let create_sql = concat!("CREATE TABLE sqlx_test_type ( col ", $datatype, " );");
                con.execute(create_sql).await?;

                $(
                    let query_result = query("INSERT INTO sqlx_test_type VALUES (?)")
                        .bind($prepared)
                        .execute(&mut *con)
                        .await?;

                    assert_eq!(query_result.rows_affected(), 1);
                    let query_str = format!("INSERT INTO sqlx_test_type VALUES (CAST ({} as {}));", $unprepared, $datatype);
                    eprintln!("{query_str}");

                    let query_result = con.execute(query_str.as_str()).await?;

                    assert_eq!(query_result.rows_affected(), 1);

                    let mut values: Vec<$ty> = query_scalar("SELECT * FROM sqlx_test_type;")
                        .fetch_all(&mut *con)
                        .await?;

                    let first_value = values.pop().unwrap();
                    let second_value = values.pop().unwrap();

                    assert_eq!(first_value, second_value, "prepared and unprepared types");
                    assert_eq!(first_value, $prepared, "provided and expected values");
                    assert_eq!(second_value, $prepared, "provided and expected values");

                    con.execute("DELETE FROM sqlx_test_type;").await?;
                )+

                Ok(())
            }
        }
    };

    ($name:ident<$ty:ty>::$datatype:literal::($($unprepared:expr),+)) => {
        $crate::types::test_type_valid!($name<$ty>::$datatype::($($unprepared => $unprepared),+));
    };

    ($name:ident::$datatype:literal::($($unprepared:expr => $prepared:expr),+)) => {
        $crate::types::test_type_valid!($name<$name>::$datatype::($($unprepared => $prepared),+));
    };

    ($name:ident::$datatype:literal::($($unprepared:expr),+)) => {
        $crate::types::test_type_valid!($name::$datatype::($($unprepared => $unprepared),+));
    };
}

    macro_rules! test_type_array {
    ($name:ident<$ty:ty>::$datatype:literal::($($prepared:expr),+)) => {
        paste::item! {
            #[sqlx::test]
            async fn [< test_type_array_ $name >] (
                mut con: sqlx_core::pool::PoolConnection<exasol::Exasol>,
            ) -> Result<(), sqlx_core::error::BoxDynError> {
                use sqlx_core::{executor::Executor, query::query, query_scalar::query_scalar};

                let create_sql = concat!("CREATE TABLE sqlx_test_type ( col ", $datatype, " );");
                con.execute(create_sql).await?;

                $(
                    let query_result = query("INSERT INTO sqlx_test_type VALUES (?)")
                        .bind($prepared)
                        .execute(&mut *con)
                        .await?;

                    let values: Vec<$ty> = query_scalar("SELECT * FROM sqlx_test_type;")
                        .fetch_all(&mut *con)
                        .await?;

                    assert_eq!(query_result.rows_affected() as usize, values.len());
                    con.execute("DELETE FROM sqlx_test_type;").await?;
                )+

                Ok(())
            }
        }
    };
}

    macro_rules! test_type_invalid {
        ($name:ident<$ty:ty>::$datatype:literal::($($prepared:expr),+)) => {
            paste::item! {
                #[sqlx::test]
                async fn [< test_type_invalid_ $name >] (
                    mut con: sqlx_core::pool::PoolConnection<exasol::Exasol>,
                ) -> Result<(), sqlx_core::error::BoxDynError> {
                    use sqlx_core::{executor::Executor, query::query, query_scalar::query_scalar};

                    let create_sql = concat!("CREATE TABLE sqlx_test_type ( col ", $datatype, " );");
                    con.execute(create_sql).await?;

                    $(
                        let query_result = query("INSERT INTO sqlx_test_type VALUES (?)")
                            .bind($prepared)
                            .execute(&mut *con)
                            .await;

                        if let Err(e) = query_result {
                            println!("Error inserting value: {e}");
                        } else {
                            let values_result: Result<Vec<$ty>, _> = query_scalar("SELECT * FROM sqlx_test_type;")
                                .fetch_all(&mut *con)
                                .await;

                            let error = values_result.unwrap_err();
                            println!("Error retrieving value: {error}");

                            con.execute("DELETE FROM sqlx_test_type;").await?;
                        }
                    )+

                    Ok(())
                }
            }
        };

}

    pub(crate) use test_type_array;
    pub(crate) use test_type_invalid;
    pub(crate) use test_type_valid;
}
