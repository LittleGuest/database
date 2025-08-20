//! sqlx 代码生成器
//!
//! 指定数据库和表名，生成对应的模型代码

use std::{collections::HashMap, fmt::Display, fs, io::Write};

use clap::Parser;
use database::{Column, Driver, Table};
use heck::ToUpperCamelCase as _;
use template::{MOD_TEMPLATE, MODEL_TEMPLATE};

use crate::template::{ERROR_TEMPLATE, RESULT_TEMPLATE};

mod template;

/// 代码生成器
///
/// Driver::Mysql       mysql://root:root@localhost:3306/test
///
/// Driver::Postgres    postgres://root:root@localhost:5432/test
///
/// Driver::Sqlite      sqlite://test.sqlite
///
#[derive(Parser, Debug)]
#[command(author, version, about,long_about = None)]
pub struct Generator {
    /// 数据库url，默认从环境变量中获取
    #[clap(short('u'), long, default_value = "")]
    pub url: String,
    /// 代码生成的路径
    #[clap(short('p'), long, default_value = "target/models/")]
    pub path: String,
    /// 指定要生成代码的表名，多个用英文逗号拼接，为空表示全部
    #[clap(short('t'), long, default_value = "")]
    pub table_names: String,
}

impl Display for Generator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            r#"
            url: {}
            path: {}
            table_names: {}
           "#,
            self.url, self.path, self.table_names
        )
    }
}

impl Generator {
    ///  处理路径，当路径不以 / 结尾时，自动添加 /
    fn deal_path(&mut self) {
        if !self.path.is_empty() && !self.path.ends_with('/') {
            self.path.push('/')
        }
    }

    /// 获取数据库驱动类型
    fn driver(&self) -> anyhow::Result<Driver> {
        Ok(Driver::from(self.url.as_str()))
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        self.deal_path();

        println!("{self}");
        println!("====== start ======");

        let (tables, tables_columns) = self.prepare().await?;
        if tables.is_empty() {
            println!("tables is empty");
            return Ok(());
        }

        if tables_columns.is_empty() {
            println!("table columns is empty");
            return Ok(());
        }
        self.write(tables, tables_columns).await?;

        println!("====== over ======");
        Ok(())
    }

    async fn prepare(&self) -> anyhow::Result<(Vec<Table>, Vec<Column>)> {
        let table_names = self
            .table_names
            .split(',')
            .filter(|t| !t.is_empty())
            .collect::<Vec<_>>();
        database::fetch_table_column(&self.url, &table_names).await
    }

    async fn write(&self, tables: Vec<Table>, tables_columns: Vec<Column>) -> anyhow::Result<()> {
        // 将tables转换为map，K：表名，V：表信息
        let table_map: HashMap<String, Table> =
            tables.into_iter().map(|t| (t.name.to_owned(), t)).collect();

        // 组装表信息和表列信息，K：表名，V：表列信息
        // FIXME：有没有办法直接将Vec分组，类似Java的Collectors.groupby
        let table_column_map =
            table_map
                .keys()
                .fold(HashMap::new(), |mut table_column_map, table_name| {
                    table_column_map.insert(
                        table_name,
                        tables_columns
                            .iter()
                            .filter(|table_column| table_column.table_name.eq(table_name))
                            .collect::<Vec<_>>(),
                    );
                    table_column_map
                });

        // 创建生成目录
        fs::create_dir_all(&self.path)?;

        // 创建模板引擎
        let mut ctx = tera::Context::new();
        ctx.insert("driver", &self.driver()?);
        ctx.insert("driver_url", &self.url);
        ctx.insert("table_names", &table_map);
        let mut tera = tera::Tera::default();

        for (table_name, table) in table_map.iter() {
            let column = table_column_map.get(&table_name);
            // 创建上下文
            ctx.insert("struct_name", &table_name.to_upper_camel_case());
            ctx.insert("table", &table);
            let mut has_columns = false;
            if let Some(columns) = column {
                has_columns = !columns.is_empty();
                ctx.insert("column_num", &columns.len());
                ctx.insert("columns", &columns);
                ctx.insert(
                    "column_names",
                    &columns
                        .iter()
                        .map(|c| c.name.clone())
                        .collect::<Vec<String>>()
                        .join(","),
                );
            }
            ctx.insert("has_columns", &has_columns);

            let contents = tera.render_str(MODEL_TEMPLATE, &ctx).expect("渲染模板错误");
            Self::write_file(&format!("{}{}.rs", self.path, &table_name), &contents).await?;
        }

        // 创建 mod.rs 文件
        let contents = tera.render_str(MOD_TEMPLATE, &ctx)?;
        Self::write_file(&format!("{}mod.rs", self.path), &contents).await?;

        // 创建 error.rs 文件
        let contents = tera.render_str(ERROR_TEMPLATE, &ctx)?;
        Self::write_file(&format!("{}error.rs", self.path), &contents).await?;

        // 创建 result.rs 文件
        let contents = tera.render_str(RESULT_TEMPLATE, &ctx)?;
        Self::write_file(&format!("{}result.rs", self.path), &contents).await?;

        Ok(())
    }

    async fn write_file(path: &str, contents: &str) -> anyhow::Result<()> {
        let mut tf = fs::File::create(path).expect("创建文件失败");
        tf.write_all(contents.as_bytes())?;
        println!("the {} has been generated", &path);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let mut generator = Generator::parse();
    // 从环境变量中获取数据库url
    if generator.url.is_empty()
        && let Ok(url) = dotenvy::var("DATABASE_URL")
    {
        generator.url = url;
    }
    if generator.url.is_empty() {
        eprintln!("database url not set");
        return;
    }
    let Err(err) = generator.run().await else {
        return;
    };
    eprintln!("{err}");
}
