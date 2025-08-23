//! sqlx 代码生成器
//!
//! 指定数据库和表名，生成对应的模型代码

use std::{
    collections::HashMap,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::anyhow;
use clap::{Parser, Subcommand};
use config::GeneratorConfig;
use database::{Column, Table, database_metadata};
use heck::ToUpperCamelCase as _;
use rust_embed::Embed;
use template::{MOD_TEMPLATE, MODEL_TEMPLATE};
use tera::Tera;

use crate::{
    config::Language,
    template::{ERROR_TEMPLATE, RESULT_TEMPLATE},
};

mod config;
mod template;

/// Rust 1.85关键字
const KEYWORDS: [&str; 53] = [
    "as", "async", "await", "break", "const", "continue", "crate", "dyn", "else", "enum", "extern",
    "false", "fn", "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub",
    "ref", "return", "Self", "self", "static", "struct", "super", "trait", "true", "type", "union",
    "unsafe", "use", "where", "while", "abstract", "become", "box", "do", "final", "gen", "macro",
    "override", "priv", "try", "typeof", "unsized", "virtual", "yield",
];

#[derive(Embed)]
#[folder = "templates/"]
struct Templates;

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
    /// 子命令
    #[command(subcommand)]
    command: Commands,
    /// 配置文件路径
    #[clap(short('c'), long, default_value = "./generator.toml")]
    pub config_path: PathBuf,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// 生成代码
    Create,
    /// 生成模板
    Template,
}

impl Generator {
    pub async fn run(&mut self, config: &mut GeneratorConfig) -> anyhow::Result<()> {
        config.deal_path();
        database::init();

        let (tables, tables_columns) = self.prepare(config).await?;
        if tables.is_empty() {
            println!("tables is empty");
            return Ok(());
        }
        if tables_columns.is_empty() {
            println!("table columns is empty");
            return Ok(());
        }

        let (tables, tables_columns) = self.prepare(config).await?;
        if tables.is_empty() {
            eprintln!("table is empty");
            return Ok(());
        }
        if tables_columns.is_empty() {
            eprintln!("table columns is empty");
            return Ok(());
        }
        self.write(&config, tables, tables_columns).await?;
        Ok(())
    }

    async fn prepare(&self, config: &GeneratorConfig) -> anyhow::Result<(Vec<Table>, Vec<Column>)> {
        let meta = database_metadata(&config.database_url).await;
        let tables = meta.tables("", &config.schema).await?;
        let table_names;
        if config.table_names.is_empty() {
            table_names = tables.iter().map(|t| t.name.clone()).collect::<Vec<_>>();
        } else {
            table_names = config.table_names.clone();
        }
        let mut columns = vec![];
        for t in table_names {
            columns.extend(meta.columns("", &config.schema, &t).await?);
        }
        Ok((tables, columns))
    }

    /// 渲染模板
    async fn render(
        &self,
        path: &str,
        tera: &mut Tera,
        ctx: &tera::Context,
    ) -> anyhow::Result<String> {
        let template = Templates::get(path).ok_or(anyhow!("模板文件不存在"))?;
        Ok(tera
            .render_str(str::from_utf8(template.data.as_ref()).unwrap(), ctx)
            .map_err(|err| anyhow!("模板渲染失败，{err}"))?)
    }

    /// 预览代码
    /// return
    ///     K：表名
    ///     V：HashMap
    ///         K：文件名
    ///         V：对应的code
    async fn preview(
        &self,
        config: &GeneratorConfig,
        tables: Vec<Table>,
        tables_columns: Vec<Column>,
    ) -> anyhow::Result<HashMap<String, HashMap<String, String>>> {
        let mut res_map = HashMap::with_capacity(config.table_names.len());

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
                            .filter(|table_column| {
                                Some(table_name.clone()) == Some(table_column.table_name.clone())
                            })
                            .collect::<Vec<_>>(),
                    );
                    table_column_map
                });
        dbg!(&table_column_map);

        // 创建模板引擎
        let mut ctx = tera::Context::new();
        ctx.insert("driver", &config.driver()?);
        ctx.insert("driver_url", &config.database_url);
        ctx.insert("table_names", &table_map);
        let mut tera = tera::Tera::default();
        match config.language {
            Language::Rust => {
                if config.gen_error {
                    let mut map = HashMap::with_capacity(1);
                    map.insert(
                        "error.rs".into(),
                        self.render("rust/error.html", &mut tera, &ctx).await?,
                    );
                    res_map.insert("error.rs".into(), map);
                }
                if config.gen_mod {
                    let mut map = HashMap::with_capacity(1);
                    map.insert(
                        "mod.rs".into(),
                        self.render("rust/mod.html", &mut tera, &ctx).await?,
                    );
                    res_map.insert("mod.rs".into(), map);
                }

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

                    let mut map = HashMap::with_capacity(3);
                    if config.gen_entity {
                        map.insert(
                            format!("{table_name}.rs"),
                            self.render("rust/model.html", &mut tera, &ctx).await?,
                        );
                    }
                    // if self.gen_service {
                    //     map.insert(
                    //         "service.rs".into(),
                    //         self.render("rust/service.html", &mut tera, &ctx)
                    //             .await?,
                    //     );
                    // }
                    // if self.gen_controller {
                    //     map.insert(
                    //         "api.rs".into(),
                    //         self.render("rust/api.html", &mut tera, &ctx).await?,
                    //     );
                    // }
                    res_map.insert(table_name.into(), map);
                }
            }
            Language::Java => {
                for table in &config.table_names {
                    let mut map = HashMap::with_capacity(6);
                    if config.gen_entity {
                        map.insert(
                            "entity.java".into(),
                            self.render("java/entity.html", &mut tera, &ctx).await?,
                        );
                    }
                    if config.gen_mapper {
                        map.insert(
                            "mapper.java".into(),
                            self.render("java/mapper.html", &mut tera, &ctx).await?,
                        );
                    }
                    if config.gen_mapper_xml {
                        map.insert(
                            "mapper.xml".into(),
                            self.render("java/mapperXml.html", &mut tera, &ctx).await?,
                        );
                    }
                    if config.gen_service {
                        map.insert(
                            "service.java".into(),
                            self.render("java/service.html", &mut tera, &ctx).await?,
                        );
                    }
                    if config.gen_controller {
                        map.insert(
                            "controller.java".into(),
                            self.render("java/controller.html", &mut tera, &ctx).await?,
                        );
                    }
                    res_map.insert(table.into(), map);
                }
            }
        }
        Ok(res_map)
    }

    /// 写入文件
    async fn write(
        &self,
        config: &GeneratorConfig,
        tables: Vec<Table>,
        tables_columns: Vec<Column>,
    ) -> anyhow::Result<()> {
        if tables.is_empty() {
            return Err(anyhow!("表信息为空"));
        }

        let data = self.preview(config, tables, tables_columns).await?;
        dbg!(&data);
        match config.language {
            Language::Rust => {
                // 创建 error.rs 文件
                if config.gen_error
                    && let Some(code) = data.get("error.rs")
                    && let Some(code) = code.get("error.rs")
                {
                    Self::write_file(
                        &format!("{}/error.rs", config.path.display()),
                        code,
                        config.r#override,
                    )
                    .await?;
                }
                // 创建 mod.rs 文件
                if config.gen_mod
                    && let Some(code) = data.get("mod.rs")
                    && let Some(code) = code.get("mod.rs")
                {
                    Self::write_file(
                        &format!("{}/mod.rs", config.path.display()),
                        code,
                        config.r#override,
                    )
                    .await?;
                }
                // 创建 model 文件
                for (key, value) in data
                    .into_iter()
                    .filter(|(k, _)| !["error.rs", "mod.rs"].contains(&k.as_str()))
                {
                    for (file_name, code) in value {
                        dbg!(format!("{}{key}/{file_name}", config.path.display()));
                        Self::write_file(
                            &format!("{}{key}/{file_name}", config.path.display()),
                            &code,
                            config.r#override,
                        )
                        .await?;
                    }
                }
            }
            Language::Java => {
                todo!()
            }
        }
        Ok(())
    }

    /// 写入文件
    async fn write_file<P>(path: P, contents: &str, r#override: bool) -> anyhow::Result<()>
    where
        P: AsRef<Path>,
    {
        if let Some(path) = path.as_ref().parent() {
            fs::create_dir_all(path)?;
        }
        if !path.as_ref().exists() || (path.as_ref().exists() && r#override) {
            let mut tf = fs::File::create(path)?;
            tf.write_all(contents.as_bytes())?;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let mut generator = Generator::parse();
    match generator.command {
        Commands::Create => {
            let config = GeneratorConfig::new(&generator.config_path);
            match config {
                Ok(mut config) => {
                    if let Err(err) = generator.run(&mut config).await {
                        eprintln!("生成代码错误，{err}");
                    }
                }
                Err(err) => {
                    eprintln!("读取配置文件错误，{err}");
                }
            }
        }
        Commands::Template => {
            if generator.config_path.exists() {
                println!("配置文件 {} 已存在", generator.config_path.display());
                return;
            }
            let Ok(mut file) = File::create(&generator.config_path) else {
                eprintln!("创建配置文件错误");
                return;
            };
            if let Err(err) = file.write_all(include_bytes!("../generator.toml")) {
                eprintln!("写入配置文件错误，{err}");
            }
        }
    }
}
