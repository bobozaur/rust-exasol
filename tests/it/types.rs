pub(crate) use macros::test_type_valid;

mod macros {
    macro_rules! test_type_valid {
    ($name:ident<$ty:ty>::$datatype:literal::($($provided:expr => $expected:expr),+)) => {
        paste::item! {
            #[sqlx::test]
            async fn [< test_type_ $name >] (
                mut con: sqlx_core::pool::PoolConnection<exasol::Exasol>,
            ) -> Result<(), sqlx_core::error::BoxDynError> {
                use sqlx_core::{executor::Executor, query::query, query_scalar::query_scalar};

                let create_sql = concat!("CREATE TABLE sqlx_test_type ( col ", $datatype, " );");
                con.execute(create_sql).await?;

                $(
                    let query_result = query("INSERT INTO sqlx_test_type VALUES (?)")
                        .bind($expected)
                        .execute(&mut *con)
                        .await?;

                    assert_eq!(query_result.rows_affected(), 1);
                    let query_str = format!("INSERT INTO sqlx_test_type VALUES (CAST ({} as {}));", $provided, $datatype);
                    eprintln!("{query_str}");

                    let query_result = con.execute(query_str.as_str()).await?;

                    assert_eq!(query_result.rows_affected(), 1);

                    let mut values: Vec<$ty> = query_scalar("SELECT * FROM sqlx_test_type;")
                        .fetch_all(&mut *con)
                        .await?;

                    let first_value = values.pop().unwrap();
                    let second_value = values.pop().unwrap();

                    assert_eq!(first_value, second_value, "prepared and unprepared types");
                    assert_eq!(first_value, $expected, "prepared and expected values");
                    assert_eq!(second_value, $expected, "unprepared and expected values");

                    con.execute("DELETE FROM sqlx_test_type;").await?;
                )+

                Ok(())
            }
        }
    };

    ($name:ident<$ty:ty>::$datatype:literal::($($provided:expr),+)) => {
        $crate::types::test_type_valid!($name<$ty>::$datatype::($($provided => $provided),+));
    };

    ($name:ident::$datatype:literal::($($provided:expr => $expected:expr),+)) => {
        $crate::types::test_type_valid!($name<$name>::$datatype::($($provided => $expected),+));
    };

    ($name:ident::$datatype:literal::($($provided:expr),+)) => {
        $crate::types::test_type_valid!($name::$datatype::($($provided => $provided),+));
    };
}

    pub(crate) use test_type_valid;
}

const MAX_U64_NUMERIC: u64 = 1000000000000000000;
const MIN_I64_NUMERIC: i64 = -999999999999999999;
const MAX_I64_NUMERIC: i64 = 1000000000000000000;

// BOOLEAN
test_type_valid!(bool::"BOOLEAN"::(false, true));

// Unsigned integers
test_type_valid!(u8::"DECIMAL(3, 0)"::(u8::MIN, u8::MAX));
test_type_valid!(u16::"DECIMAL(5, 0)"::(u16::MIN, u16::MAX, u16::from(u8::MAX)));
test_type_valid!(u32::"DECIMAL(10, 0)"::(u32::MIN, u32::MAX, u32::from(u8::MAX), u32::from(u16::MAX)));
test_type_valid!(u64::"DECIMAL(20, 0)"::(u64::MIN, u64::MAX, u64::from(u8::MAX), u64::from(u16::MAX), u64::from(u32::MAX), MAX_U64_NUMERIC, MAX_U64_NUMERIC - 1));
test_type_valid!(u64_empty<Option<u64>>::"DECIMAL(20, 0)"::("NULL" => None::<u64>));

// Signed integers
test_type_valid!(i8::"DECIMAL(3, 0)"::(i8::MIN, i8::MAX));
test_type_valid!(i16::"DECIMAL(5, 0)"::(i16::MIN, i16::MAX, i16::from(i8::MIN), i16::from(i8::MAX)));
test_type_valid!(i32::"DECIMAL(10, 0)"::(i32::MIN, i32::MAX, i32::from(i8::MIN), i32::from(i8::MAX), i32::from(i16::MIN), i32::from(i16::MAX)));
test_type_valid!(i64::"DECIMAL(20, 0)"::(i64::MIN, i64::MAX, i64::from(i8::MIN), i64::from(i8::MAX), i64::from(i16::MIN), i64::from(i16::MAX), i64::from(i32::MIN), i64::from(i32::MAX), MIN_I64_NUMERIC, MIN_I64_NUMERIC - 1, MAX_I64_NUMERIC, MAX_I64_NUMERIC - 1));
test_type_valid!(i64_empty<Option<i64>>::"DECIMAL(20, 0)"::("NULL" => None::<i64>));

// Floats (and their weirdness)
test_type_valid!(f32::"DOUBLE PRECISION"::(f32::MIN, f32::MAX));
test_type_valid!(f64::"DOUBLE PRECISION"::(-3.40282346638529e38f64, 3.40282346638529e38f64));
test_type_valid!(f32_decimal<f32>::"DECIMAL(36, 16)"::(-1005.0456, 1005.0456, -7462.0, 7462.0));
test_type_valid!(f64_decimal<f64>::"DECIMAL(36, 16)"::(-1005213.0456543, 1005213.0456543, -1005.0456, 1005.0456, -7462.0, 7462.0));
test_type_valid!(f64_empty<Option<f64>>::"DECIMAL(36, 16)"::("NULL" => None::<f64>));

// Strings
test_type_valid!(varchar_ascii<String>::"VARCHAR(100) ASCII"::("'first value'" => "first value", "'second value'" => "second value"));
test_type_valid!(varchar_utf8<String>::"VARCHAR(100) UTF8"::("'first value 🦀'" => "first value 🦀", "'second value 🦀'" => "second value 🦀"));
test_type_valid!(char_ascii<String>::"CHAR(10) ASCII"::("'first     '" => "first     ", "'second'" => "second    "));
test_type_valid!(char_utf8<String>::"CHAR(10) UTF8"::("'first 🦀   '" => "first 🦀   ", "'second 🦀'" => "second 🦀  "));

test_type_valid!(varchar_empty<Option<String>>::"VARCHAR(100) UTF8"::("''" => None::<String>, "NULL" => None::<String>));
test_type_valid!(char_empty<Option<String>>::"CHAR(100) UTF8"::("''" => None::<String>, "NULL" => None::<String>));

#[cfg(feature = "rust_decimal")]
mod rust_decimal_tests {
    use super::*;
    use rust_decimal::Decimal;

    test_type_valid!(rust_decimal_i64<Decimal>::"DECIMAL(36, 16)"::(Decimal::new(i64::MIN, 16), Decimal::new(i64::MAX, 16), Decimal::new(i64::MAX, 10), Decimal::new(i64::MAX, 5), Decimal::new(i64::MAX, 0)));
    test_type_valid!(rust_decimal_i16<Decimal>::"DECIMAL(36, 16)"::(Decimal::new(i64::from(i16::MIN), 5), Decimal::new(i64::from(i16::MAX), 5), Decimal::new(i64::from(i16::MIN), 0), Decimal::new(i64::from(i16::MAX), 0)));
    test_type_valid!(rust_decimal_no_scale<Decimal>::"DECIMAL(36, 0)"::(Decimal::new(-340282346638529, 0), Decimal::new(340282346638529, 0), Decimal::new(0, 0)));
    test_type_valid!(rust_decimal_empty<Option<Decimal>>::"DECIMAL(36, 0)"::("NULL" => None::<Decimal>));
}
