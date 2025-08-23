use serde::{Deserialize, Serialize};
use sqlx::{AnyPool, FromRow, PgPool, Row, any::AnyRow, postgres::PgRow};

use super::{DatabaseMetadata, Result};

pub struct PostgresMetadata(PgPool);

#[derive(Debug, Serialize, Deserialize, FromRow)]
struct Database {
    name: String,
}

impl From<Database> for super::Database {
    fn from(d: Database) -> Self {
        Self { name: d.name }
    }
}

#[derive(Debug, Serialize, Deserialize, FromRow)]
struct Schema {
    name: String,
}

impl From<Schema> for super::Schema {
    fn from(s: Schema) -> Self {
        Self { name: s.name }
    }
}

#[derive(Default, Debug, Serialize, Deserialize, FromRow)]
#[serde(rename_all = "camelCase")]
struct Table {
    /// 表所属的数据库名称
    // #[sqlx(rename = "table_catalog")]
    table_catalog: String,
    /// 表所属的模式名称 (如 public)
    // #[sqlx(rename = "table_schema")]
    table_schema: String,
    /// 表的名称
    // #[sqlx(rename = "table_name")]
    table_name: String,
    // /// 表的类型
    // /// - 'BASE TABLE': 普通表
    // /// - 'VIEW': 视图
    // /// - 'FOREIGN TABLE': 外部表
    // /// - 'LOCAL TEMPORARY': 临时表
    // // #[sqlx(rename = "table_type")]
    // table_type: String,
    // /// 自引用列的名称 (通常为 NULL)
    // // #[sqlx(rename = "self_referencing_column_name")]
    // self_referencing_column_name: Option<String>,
    // /// 引用生成方式 (如 'SYSTEM GENERATED'，通常为 NULL)
    // // #[sqlx(rename = "reference_generation")]
    // reference_generation: Option<String>,
    // /// 用户定义类型所属的数据库 (通常为 NULL)
    // // #[sqlx(rename = "user_defined_type_catalog")]
    // user_defined_type_catalog: Option<String>,
    // /// 用户定义类型所属的模式 (通常为 NULL)
    // // #[sqlx(rename = "user_defined_type_schema")]
    // user_defined_type_schema: Option<String>,
    // /// 用户定义类型的名称 (通常为 NULL)
    // // #[sqlx(rename = "user_defined_type_name")]
    // user_defined_type_name: Option<String>,
    // /// 是否可向表中插入数据 ('YES'/'NO')
    // // #[sqlx(rename = "is_insertable_into")]
    // is_insertable_into: String,
    // /// 是否为类型化表 ('YES'/'NO')
    // // #[sqlx(rename = "is_typed")]
    // is_typed: String,
    // /// 提交动作 (PostgreSQL 中通常为 NULL)
    // // #[sqlx(rename = "commit_action")]
    // commit_action: Option<String>,
    /// 描述
    description: Option<String>,
}

impl From<Table> for super::Table {
    fn from(t: Table) -> Self {
        Self {
            schema: t.table_schema,
            name: t.table_name.clone(),
            comment: t.description.unwrap_or(t.table_name),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, FromRow)]
#[serde(rename_all = "camelCase")]
struct Column {
    /// 列所属的数据库名称
    table_catalog: String,
    /// 列所属的模式名称 (如 public)
    table_schema: String,
    /// 列所属的表名称
    table_name: String,
    /// 列的名称
    column_name: String,
    /// 列在表中的位置序号 (从1开始)
    ordinal_position: i32,
    /// 列的默认值表达式
    column_default: Option<String>,
    /// 列是否允许 NULL 值
    is_nullable: String, // 'YES' or 'NO'
    /// 列的标准SQL数据类型
    data_type: String,
    /// 字符类型列的最大长度
    character_maximum_length: Option<i32>,
    // /// 字符类型列的最大字节长度
    // character_octet_length: Option<i32>,
    // /// 数值类型列的精度
    // numeric_precision: Option<i32>,
    // /// 数值精度的基数 (2=二进制, 10=十进制)
    // numeric_precision_radix: Option<i32>,
    // /// 数值类型列的小数位数
    // numeric_scale: Option<i32>,
}

impl From<Column> for super::Column {
    fn from(c: Column) -> Self {
        let ty = t2t(&c.data_type.clone().to_uppercase()).to_string();
        Self {
            database: c.table_catalog,
            schema: c.table_schema,
            table_name: c.table_name,
            name: c.column_name,
            // r#type: Some(ty),
            length: c.character_maximum_length,
            default: c.column_default,
            // enum_values: todo!(),
            // comment: todo!(),
            // is_null: todo!(),
            // is_auto_incr: todo!(),
            // is_unique: todo!(),
            // is_primary_key: todo!(),
            // is_unsigned: todo!(),
            // rust_type: todo!(),
            ..Default::default()
        }
        // Self {
        //     schema: Some(c.table_schema.clone()),
        //     table_name: Some(c.table_name.clone()),
        //     name: Some(super::column_keywords(c.column_name.clone().as_str())),
        //     default: c.column_default.clone(),
        //     is_nullable: {
        //         if ty.contains("Time") {
        //             true
        //         } else {
        //             c.is_nullable.eq_ignore_ascii_case("yes")
        //         }
        //     },
        //     column_type: Some(c.data_type),
        //     comment: c.description,
        //     field_type: ty,
        //     // multi_world: Some(c.column_name.clone().contains(|c| c == '_' || c == '-')),
        //     max_length: {
        //         if let Some(l) = c.character_maximum_length {
        //             Some(l as i64)
        //         } else {
        //             Some(50)
        //         }
        //     },
        // }
    }
}

/// Rust type            Postgres type(s)
/// bool                    BOOL
/// i8                      “CHAR”
/// i16                     SMALLINT, SMALLSERIAL, INT2
/// i32                     INT, SERIAL, INT4
/// i64                     BIGINT, BIGSERIAL, INT8
/// f32                     REAL, FLOAT4
/// f64                     DOUBLE PRECISION, FLOAT8
/// &str, String            VARCHAR, CHAR(N), TEXT, NAME
/// &[u8], Vec<u8>          BYTEA
/// ()                      VOID
/// PgInterval              INTERVAL
/// PgRange<T>              INT8RANGE, INT4RANGE, TSRANGE, TSTZRANGE, DATERANGE, NUMRANGE
/// PgMoney                 MONEY
/// PgLTree                 LTREE
/// PgLQuery                LQUERY
///
/// bigdecimal::BigDecimal  NUMERIC
///
/// time::PrimitiveDateTime TIMESTAMP
/// time::OffsetDateTime    TIMESTAMPTZ
/// time::Date              DATE
/// time::Time              TIME
/// [PgTimeTz]              TIMETZ
///
/// uuid::Uuid              UUID
///
/// ipnetwork::IpNetwork    INET, CIDR
/// std::net::IpAddr        INET, CIDR
///
/// mac_address::MacAddress MACADDR
///
/// bit_vec::BitVec         BIT, VARBIT
///
/// serde_json::Value       JSON, JSONB
///
/// PostgreSQL 类型转换为Rust对应类型
fn t2t(ty: &str) -> &str {
    match ty.to_uppercase().as_str() {
        "BOOL" => "bool",
        "CHAR" => "i8",
        "SMALLINT" | "SMALLSERIAL" | "INT2" => "i16",
        "INT" | "SERIAL" | "INT4" => "i32",
        "BIGINT" | "BIGSERIAL" | "INT8" => "i64",
        "REAL" | "FLOAT4" => "f32",
        "DOUBLE PRECISION" | "FLOAT8" => "f64",
        "BYTEA" => "Vec<u8>",
        "VOID" => "()",
        "INTERVAL" => "sqlx_postgres::types::PgInterval",
        "INT8RANGE" | "INT4RANGE" | "TSRANGE" | "TSTZRANGE" | "DATERANGE" | "NUMRANGE" => {
            "sqlx_postgres::types::PgRange<T> "
        }
        "MONEY" => "sqlx_postgres::types::PgMoney",
        "LTREE" => "sqlx_postgres::types::PgLTree",
        "LQUERY" => "sqlx_postgres::types::PgLQuery",
        "YEAR" => "time::Date",
        "DATE" => "time::Date",
        "TIME" => "time::Time",
        "TIMESTAMP" => "time::PrimitiveDateTime",
        "TIMESTAMPTZ" => "time::OffsetDateTime",
        "TIMETZ" => "sqlx_postgres::types::PgTimeTz",
        "NUMERIC" => "bigdecimal::BigDecimal",
        "JSON" | "JSONB" => "serde_json:JsonValue",
        "UUID" => "uuid::Uuid",
        "INET" | "CIDR" => "std::net::IpAddr",
        "MACADDR" => "mac_address::MacAddress",
        "BIT" | "VARBIT" => "bit_vec::BitVec",
        _ => "String",
    }
}

impl PostgresMetadata {
    pub fn new(pool: PgPool) -> Self {
        Self(pool)
    }
}

impl DatabaseMetadata for PostgresMetadata {
    fn databases(&self) -> super::BoxFuture<'_, Result<Vec<super::Database>>> {
        todo!()
    }
    fn schemas(&self) -> super::BoxFuture<'_, Result<Vec<super::Schema>>> {
        todo!()
    }

    fn tables<'a>(
        &'a self,
        database: &'a str,
        schema: &'a str,
    ) -> super::BoxFuture<'a, Result<Vec<super::Table>>> {
        let mut sql = "SELECT tb.table_catalog, tb.table_schema, tb.TABLE_NAME, d.description FROM information_schema.tables tb JOIN pg_class C ON C.relname = tb. TABLE_NAME LEFT JOIN pg_description d ON d.objoid = C.OID  AND d.objsubid = '0' where ".to_string();
        if database.is_empty() {
            sql.push_str(" tb.table_catalog = current_database()");
        } else {
            sql.push_str(" tb.table_catalog = $1");
        }
        sql.push_str(" and tb.table_schema = $2");

        Box::pin(async move {
            let rows: Vec<Table> = sqlx::query_as(&sql)
                .bind(database)
                .bind(schema)
                .fetch_all(&self.0)
                .await?;
            Ok(rows.into_iter().map(|row| row.into()).collect::<Vec<_>>())
        })
    }

    fn columns<'a>(
        &'a self,
        database: &'a str,
        schema: &'a str,
        table_name: &'a str,
    ) -> super::BoxFuture<'a, Result<Vec<super::Column>>> {
        let mut sql = "
        SELECT
        	col.table_catalog,
        	col.table_schema,
        	col.TABLE_NAME,
        	col.COLUMN_NAME,
        	col.ordinal_position,
        	col.column_default,
        	col.is_nullable,
        	col.udt_name as data_type,
        	col.character_maximum_length,
        	d.description
        FROM
        	information_schema.COLUMNS col
        	JOIN pg_class C ON C.relname = col.
        	TABLE_NAME LEFT JOIN pg_description d ON d.objoid = C.OID
        	AND d.objsubid = col.ordinal_position
        WHERE
       "
        .to_string();

        if database.is_empty() {
            sql.push_str(" col.table_catalog = current_database() ");
        } else {
            sql.push_str(" col.table_catalog = $1 ");
        }
        if schema.is_empty() {
            sql.push_str(" and col.table_schema = current_schema() ");
        } else {
            sql.push_str(" and col.table_schema = $2 ");
        }
        sql.push_str(" and col.TABLE_NAME = $3 ");
        sql.push_str(" ORDER BY col.TABLE_NAME, col.ordinal_position ");

        Box::pin(async move {
            let rows: Vec<Column> = sqlx::query_as(&sql)
                .bind(database)
                .bind(schema)
                .bind(table_name)
                .fetch_all(&self.0)
                .await?;
            Ok(rows.into_iter().map(|row| row.into()).collect::<Vec<_>>())
        })
    }

    fn indexs<'a>(
        &'a self,
        database: &'a str,
        schema: &'a str,
        table_name: &'a str,
    ) -> super::BoxFuture<'a, Result<Vec<super::Index>>> {
        todo!()
    }

    fn create_table_sql<'a>(
        &'a self,
        database: &'a str,
        schema: &'a str,
        table_name: &'a str,
    ) -> super::BoxFuture<'a, Result<String>> {
        todo!()
    }
}