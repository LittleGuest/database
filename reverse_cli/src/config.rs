use std::{
    collections::HashMap,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::anyhow;
use clap::{Parser, Subcommand};
use database::{Column, Driver, Table, database_metadata};
use heck::ToUpperCamelCase as _;
use rust_embed::Embed;
use serde::{Deserialize, Serialize};
use tera::Tera;

use crate::template::{ERROR_TEMPLATE, RESULT_TEMPLATE};

/// 支持的编程语言
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Language {
    Rust,
    Java,
}

/// 代码生成器配置
#[derive(Debug, Serialize, Deserialize)]
pub struct GeneratorConfig {
    /// 数据源
    pub database_url: String,
    /// 数据库模式
    pub schema: String,
    /// 编程语言
    pub language: Language,
    /// 指定要生成代码的表名，为空表示全部
    pub table_names: Vec<String>,
    /// 忽略的表名
    pub ignore_tables: Vec<String>,
    /// 忽略表名前缀
    pub ignore_table_prefix: Option<String>,
    /// 代码生成的路径
    pub path: PathBuf,
    /// 是否覆盖
    pub r#override: bool,

    /// 是否生成 mod.rs 文件
    pub gen_mod: bool,
    /// 是否生成 error.rs 文件
    pub gen_error: bool,
    /// 是否生成 Entity 文件
    pub gen_entity: bool,
    /// 是否生成 Mapper 文件
    pub gen_mapper: bool,
    /// 是否生成 MapperXml 文件
    pub gen_mapper_xml: bool,
    /// 是否生成 Service 文件
    pub gen_service: bool,
    /// 是否生成 Controller 文件
    pub gen_controller: bool,

    /// entity的包名
    pub entity_package_name: Option<String>,
    /// mapper的包名
    pub mapper_package_name: Option<String>,
    /// mapperXml的包名
    pub mapper_xml_package_name: Option<String>,
    /// service的包名
    pub service_package_name: Option<String>,
    /// serviceImpl的包名
    pub service_impl_package_name: Option<String>,
    /// controller的包名
    pub controller_package_name: Option<String>,
}

impl TryFrom<&str> for GeneratorConfig {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let config = toml::from_str(value)?;
        Ok(config)
    }
}

impl GeneratorConfig {
    /// 从配置文件创建 GeneratorConfig
    pub fn new<P: AsRef<Path>>(config_path: P) -> anyhow::Result<Self> {
        let data = fs::read_to_string(config_path)?;
        let config = GeneratorConfig::try_from(data.as_str())?;
        Ok(config)
    }
    
    /// 获取数据库驱动类型
    pub fn driver(&self) -> anyhow::Result<Driver> {
        Driver::try_from(self.database_url.as_str()).map_err(|_| anyhow!("数据库驱动类型不支持"))
    }

    ///  处理路径，当路径不以 / 结尾时，自动添加 /
    pub fn deal_path(&mut self) {
        // if !self.path.is_empty() && !self.path.ends_with('/') {
        //     self.path.push('/')
        // }
    }
}
