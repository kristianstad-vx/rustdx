use crate::cmd::DayCmd;
use eyre::{anyhow, Result};
use rustdx::file::{
    day::fq::Day,
    gbbq::{Factor, Gbbq},
};
use rustdx_cmd::fetch_code::StockList;
use std::{
    fs::{self, File},
    io::{self, Write},
    path::Path,
    process::Command,
};
use crate::cmd::GbbqCmd;
use rustdx::file::gbbq::Gbbqs;
use chrono::NaiveDate;

const BUFFER_SIZE: usize = 32 * (1 << 20); // 32M

/// TODO 协程解析、异步缓冲写入（利用多核优势）
pub fn run_csv(cmd: &DayCmd) -> Result<()> {
    let hm = cmd.stocklist();
    let file = File::create(&cmd.output)?;
    let mut wtr = csv::WriterBuilder::new()
        .buffer_capacity(BUFFER_SIZE)
        .from_writer(file);
    for dir in &cmd.path {
        let n = filter_file(dir)?.count();
        info!("dir: {dir:?} day 文件数量：{n}");
        let take = cmd.amount.unwrap_or(n);

        let mut count: usize = 0;
        filter_file(dir)?
            .map(|f| (cmd.filter_ec(f.to_str().unwrap()), f))
            .filter(|((b, _), s)| filter(*b, s, hm.as_ref(), dir).unwrap_or(false))
            .take(take)
            .filter_map(|((_, code), src)| {
                count += 1;
                debug!("#{code:06}# {src:?}");
                rustdx::file::day::Day::from_file_into_vec(code, src).ok()
            })
            .flatten()
            .try_for_each(|t| wtr.serialize(t))?;

        print(dir, count, take);
    }
    wtr.flush().map_err(|e| e.into())
}

/// TODO 协程解析、异步缓冲写入（利用多核优势）
pub fn run_csv_fq(cmd: &DayCmd) -> Result<()> {
    // 股本变迁
    let mut bytes = fs::read(cmd.gbbq.as_ref().unwrap())?;
    let gbbq = Gbbq::filter_hashmap(Gbbq::iter(&mut bytes[4..]));

    // 股票列表
    let hm = cmd.stocklist();

    let file = File::create(&cmd.output)?;
    let mut wtr = csv::WriterBuilder::new()
        .buffer_capacity(BUFFER_SIZE)
        .from_writer(file);
    for dir in &cmd.path {
        let n = filter_file(dir)?.count();
        info!("dir: {dir:?} day 文件数量：{n}");
        let take = cmd.amount.unwrap_or(n);

        let mut count: usize = 0;
        filter_file(dir)?
            .map(|f| (cmd.filter_ec(f.to_str().unwrap()), f))
            .filter(|((b, _), s)| filter(*b, s, hm.as_ref(), dir).unwrap_or(false))
            .take(take)
            .filter_map(|((_, code), src)| {
                count += 1;
                debug!("#{code:06}# {src:?}");
                Day::new(code, src, gbbq.get(&code).map(Vec::as_slice)).ok()
            })
            .flatten()
            .try_for_each(|t| wtr.serialize(t))?;

        print(dir, count, take);
    }
    wtr.flush().map_err(|e| e.into())
}

/// TODO 协程解析、异步缓冲写入（利用多核优势）
pub fn run_csv_fq_previous(cmd: &DayCmd) -> Result<()> {
    // 股本变迁
    let mut bytes = fs::read(cmd.gbbq.as_ref().unwrap())?;
    let gbbq = Gbbq::filter_hashmap(Gbbq::iter(&mut bytes[4..]));

    // 前收
    let previous = previous_csv_table(&cmd.previous, &cmd.table, cmd.keep_factor)?;

    // 股票列表
    let hm = cmd.stocklist();

    let file = File::create(&cmd.output)?;
    let mut wtr = csv::WriterBuilder::new()
        .buffer_capacity(BUFFER_SIZE)
        .from_writer(file);
    for dir in &cmd.path {
        let n = filter_file(dir)?.count();
        info!("dir: {dir:?} day 文件数量：{n}");
        let take = cmd.amount.unwrap_or(n);

        let mut count: usize = 0;
        filter_file(dir)?
            .map(|f| (cmd.filter_ec(f.to_str().unwrap()), f))
            .filter(|((b, _), s)| filter(*b, s, hm.as_ref(), dir).unwrap_or(false))
            .take(take)
            .filter_map(|((_, code), src)| {
                count += 1;
                debug!("#{code:06}# {src:?}");
                Day::concat(
                    code,
                    src,
                    // 无分红数据并不意味着无复权数据
                    gbbq.get(&code).map(Vec::as_slice),
                    previous.get(&code),
                )
                .ok()
            })
            .flatten()
            .try_for_each(|t| wtr.serialize(t))?;

        print(dir, count, take);
    }
    wtr.flush().map_err(|e| e.into())
}

/// 筛选 day 文件
#[rustfmt::skip]
fn filter_file(dir: &Path) -> Result<impl Iterator<Item = std::path:: PathBuf>> {
    Ok(dir.read_dir()?
          .filter_map(|e| e.map(|f| f.path()).ok())
          .filter(|p| p.extension().map(|s| s == "day").unwrap_or_default()))
}

/// 筛选存在于股票列表的股票
#[inline]
fn filter(b: bool, src: &Path, hm: Option<&StockList>, dir: &Path) -> Option<bool> {
    let src = src.strip_prefix(dir).ok()?.to_str()?.strip_suffix(".day")?;
    Some(b && hm.map(|m| m.contains(src)).unwrap_or(true))
}

fn print(dir: &Path, count: usize, take: usize) {
    if count == 0 && take != 0 {
        error!("{dir:?} 目录下无 `.day` 文件符合要求");
    } else if take == 0 {
        error!("请输入大于 0 的文件数量");
    } else {
        info!("{dir:?}\t已完成：{count}");
    }
}

fn database_table(table: &str) -> (&str, &str) {
    let pos = table.find('.').unwrap();
    table.split_at(pos) // (database_name, table_name)
}

pub fn setup_clickhouse(fq: bool, table: &str) -> Result<()> {
    let create_database = format!("CREATE DATABASE IF NOT EXISTS {}", database_table(table).0);
    let output = Command::new("clickhouse-client")
        .args(["--query", &create_database])
        .output()?;
    check_output(output);
    #[rustfmt::skip]
    let create_table = if fq {
        format!("
            CREATE TABLE IF NOT EXISTS {table}
            (
                `date` Date CODEC(DoubleDelta),
                `code` FixedString(6),
                `open` Float32,
                `high` Float32,
                `low` Float32,
                `close` Float32,
                `amount` Float64,
                `vol` Float64,
                `preclose` Float64,
                `factor` Float64
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (date, code)
        ")
    } else {
        format!("
            CREATE TABLE IF NOT EXISTS {table}
            (
                `date` Date CODEC(DoubleDelta),
                `code` FixedString(6),
                `open` Float32,
                `high` Float32,
                `low` Float32,
                `close` Float32,
                `amount` Float64,
                `vol` Float64
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (date, code)
        ")
    }; // PARTITION BY 部分可能需要去掉
    let output = Command::new("clickhouse-client")
        .args(["--query", &create_table])
        .output()?;
    check_output(output);
    Ok(())
}

pub fn insert_clickhouse(output: &impl AsRef<Path>, table: &str, keep: bool) -> Result<()> {
    use subprocess::{Exec, Redirection};
    let query = format!("INSERT INTO {table} FORMAT CSVWithNames");
    let capture = Exec::cmd("clickhouse-client")
        .args(&["--query", &query])
        .stdin(Redirection::File(File::open(output)?))
        .capture()?;
    if capture.success() {
        info!("成功插入数据到 clickhouse 数据库");
        debug!("clickhouse 返回结果：{}", capture.stdout_str());
    } else {
        error!(
            "插入数据到 clickhouse 数据库时遇到：{}",
            capture.stderr_str()
        );
    };
    keep_csv(output, keep)?;
    Ok(())
}

/// 需要日线 clickhouse csv 文件
#[test]
fn test_insert_clickhouse() -> Result<()> {
    setup_clickhouse(true, "rustdx.tmp")?;
    insert_clickhouse(&"clickhouse", "rustdx.tmp", true)
}

type Previous = Result<std::collections::HashMap<u32, Factor>>;

pub fn previous_csv_table(
    path: &Option<std::path::PathBuf>,
    table: &str,
    keep_factor: bool,
) -> Previous {
    if let Some(Some(path)) = path.as_ref().map(|p| p.to_str()) {
        if path == "clickhouse" {
            clickhouse_factor_csv(table, keep_factor)
        } else {
            previous_csv(path, keep_factor)
        }
    } else {
        Err(anyhow!("请检查 gbbq 路径"))
    }
}

/// 读取前收盘价（前 factor ）数据
pub fn previous_csv(p: impl AsRef<Path>, keep_factor: bool) -> Previous {
    let path = p.as_ref();
    let prev = csv::Reader::from_reader(File::open(path)?)
        .deserialize::<Factor>()
        .filter_map(|f| f.ok())
        .map(|f| (f.code.parse().unwrap(), f))
        .collect();
    if !keep_factor {
        fs::remove_file(path)?;
    }
    Ok(prev)
}

/// 获取当前最新 factor
fn clickhouse_factor_csv(table: &str, keep_factor: bool) -> Previous {
    let query = format!(
        "\
WITH
  df AS (
  SELECT
    code,
  arrayLast(
      x->true,
      arraySort(x->x.1, groupArray((
        date, close, factor
      )))
    ) AS t
  FROM
    {table}
  GROUP BY
    code
  )
SELECT code, t.1 AS date, t.2 AS close, t.3 AS factor FROM df
INTO OUTFILE 'factor.csv'
FORMAT CSVWithNames;"
    );
    let args = ["--query", &query];
    let output = Command::new("clickhouse-client").args(args).output()?;
    info!("clickhouse-client --query {query:?}");
    check_output(output);
    previous_csv("factor.csv", keep_factor)
}

/// TODO: 与数据库有关的，把库名、表名可配置
pub fn run_mongodb(cmd: &DayCmd) -> Result<()> {
    cmd.run_csv()?;
    // TODO:排查为什么 date 列无法变成 date 类型 date.date(2006-01-02)
    let (database_name, table_name) = database_table(&cmd.table);
    let args = [
        "--db",
        database_name,
        "--collection",
        table_name,
        "--type=csv",
        "--file",
        &cmd.output,
        "--columnsHaveTypes",
        "--fields=code.string()",
    ];
    let output = Command::new("mongoimport").args(args).output()?;
    check_output(output);
    keep_csv(&cmd.output, cmd.keep_csv)?;
    Ok(())
}

fn check_output(output: std::process::Output) {
    io::stdout().write_all(&output.stdout).unwrap();
    io::stderr().write_all(&output.stderr).unwrap();
    assert!(output.status.success());
}

fn keep_csv(fname: &impl AsRef<Path>, keep: bool) -> io::Result<()> {
    if keep {
        fs::rename(fname, fname.as_ref().with_extension("csv"))
    } else {
        fs::remove_file(fname)
    }
}

/// 读取本地 xls(x) 文件
pub fn read_xlsx(path: &str, col: usize, prefix: &str) -> Option<StockList> {
    use calamine::{open_workbook_auto, Data, Reader};
    let mut workbook = open_workbook_auto(path).ok()?;
    let format_ = |x: &str| format!("{}{}", crate::cmd::auto_prefix(prefix, x), x);
    // 每个单元格被解析的类型可能会不一样，所以把股票代码统一转化成字符型
    if let Some(Ok(range)) = workbook.worksheet_range_at(0) {
        Some(
            range
                .rows()
                .skip(1)
                .map(|r| match &r[col] {
                    Data::Int(x) => format_(&x.to_string()),
                    Data::Float(x) => format_(&(*x as i64).to_string()),
                    Data::String(x) => format_(x),
                    _ => unreachable!(),
                })
                .collect(),
        )
    } else {
        None
    }
}

// Custom struct for GBBQ CSV output
#[derive(Debug, serde::Serialize)]
struct GbbqCsvRecord {
    market: String,
    code: String,
    date: String,
    category: u8,
    category_name: String,
    fh_qltp: f32,
    pgj_qzgb: f32,
    sg_hltp: f32,
    pg_hzgb: f32,
}

impl GbbqCsvRecord {
    fn from_gbbq(gbbq: &Gbbq) -> Self {
        Self {
            market: match gbbq.market {
                1 => "SZ".to_string(),
                2 => "SH".to_string(),
                _ => gbbq.market.to_string(),
            },
            code: gbbq.code.to_string(),
            date: format_gbbq_date(gbbq.date),
            category: gbbq.category,
            category_name: get_gbbq_category_name(gbbq.category),
            fh_qltp: gbbq.fh_qltp,
            pgj_qzgb: gbbq.pgj_qzgb,
            sg_hltp: gbbq.sg_hltp,
            pg_hzgb: gbbq.pg_hzgb,
        }
    }
}

/// Parse and export GBBQ data to CSV
pub fn run_gbbq_csv(cmd: &GbbqCmd) -> Result<()> {
    info!("🚀 Starting GBBQ file parsing...");

    // Check if file exists
    if !cmd.gbbq_file.exists() {
        return Err(anyhow!("GBBQ file not found: {:?}", cmd.gbbq_file));
    }

    // Load and parse GBBQ file
    info!("📁 Loading GBBQ file: {:?}", cmd.gbbq_file);
    let mut gbbqs = Gbbqs::from_file(&cmd.gbbq_file)?;
    info!("📊 Found {} records in the file", gbbqs.count);

    // Parse all records
    info!("🔓 Decrypting and parsing records...");
    let gbbq_records = gbbqs.to_vec();
    info!("✅ Successfully parsed {} records", gbbq_records.len());

    // Apply filters
    let filtered_records = apply_gbbq_filters(&gbbq_records, cmd);
    info!("🔍 After filtering: {} records", filtered_records.len());

    // Create CSV writer
    info!("📝 Creating CSV file: {}", cmd.output);
    let file = File::create(&cmd.output)?;
    let mut wtr = csv::WriterBuilder::new()
        .buffer_capacity(BUFFER_SIZE)
        .from_writer(file);

    // Write records to CSV
    let mut written_count = 0;
    let mut category_stats = std::collections::HashMap::new();

    for record in &filtered_records {
        let csv_record = GbbqCsvRecord::from_gbbq(record);
        wtr.serialize(&csv_record)?;
        written_count += 1;

        // Collect statistics
        *category_stats.entry(record.category).or_insert(0) += 1;

        // Progress indicator for large files
        if written_count % 1000 == 0 {
            info!("📝 Written {} records...", written_count);
        }
    }

    wtr.flush()?;

    // Display results
    info!("🎉 Success! GBBQ data saved to: {}", cmd.output);
    info!("📈 Total records written: {}", written_count);

    // Display category statistics
    if !category_stats.is_empty() {
        info!("📊 Records by category:");
        let mut sorted_categories: Vec<_> = category_stats.iter().collect();
        sorted_categories.sort_by_key(|(category, _)| *category);

        for (category, count) in sorted_categories {
            info!("  Category {}: {} records - {}",
                  category, count, get_gbbq_category_name(*category));
        }
    }

    Ok(())
}

/// Apply filters based on command options
fn apply_gbbq_filters<'a>(records: &'a [Gbbq], cmd: &GbbqCmd) -> Vec<&'a Gbbq<'a>> {
    records.iter()
        .filter(|record| {
            // Filter by category
            if let Some(category) = cmd.category {
                if record.category != category {
                    return false;
                }
            }

            // Filter by stock codes
            if let Some(stocks) = cmd.parse_stocks() {
                if !stocks.contains(&record.code.to_string()) {
                    return false;
                }
            }

            // Filter by date range
            if let Some((start_date, end_date)) = cmd.parse_date_range() {
                if record.date < start_date || record.date > end_date {
                    return false;
                }
            }

            true
        })
        .collect()
}

/// Setup ClickHouse table for GBBQ data
pub fn setup_gbbq_clickhouse(table: &str) -> Result<()> {
    let create_database = format!("CREATE DATABASE IF NOT EXISTS {}", database_table(table).0);
    let output = Command::new("clickhouse-client")
        .args(["--query", &create_database])
        .output()?;
    check_output(output);

    let create_table = format!(
        "CREATE TABLE IF NOT EXISTS {table}
        (
            `market` String,
            `code` FixedString(6),
            `date` Date,
            `category` UInt8,
            `category_name` String,
            `fh_qltp` Float32,
            `pgj_qzgb` Float32,
            `sg_hltp` Float32,
            `pg_hzgb` Float32
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY (date, code, category)"
    );

    let output = Command::new("clickhouse-client")
        .args(["--query", &create_table])
        .output()?;
    check_output(output);
    Ok(())
}

/// Convert YYYYMMDD integer to YYYY-MM-DD string
fn format_gbbq_date(date: u32) -> String {
    let year = date / 10000;
    let month = (date % 10000) / 100;
    let day = date % 100;

    if let Some(naive_date) = NaiveDate::from_ymd_opt(year as i32, month, day) {
        naive_date.format("%Y-%m-%d").to_string()
    } else {
        format!("{:04}-{:02}-{:02}", year, month, day)
    }
}

/// Get human-readable category name
fn get_gbbq_category_name(category: u8) -> String {
    match category {
        1 => "除权除息".to_string(),
        2 => "送配股上市".to_string(),
        3 => "非流通股上市".to_string(),
        4 => "未知股本变动".to_string(),
        5 => "股本变化".to_string(),
        6 => "增发新股".to_string(),
        7 => "股份回购".to_string(),
        8 => "增发新股上市".to_string(),
        9 => "转配股上市".to_string(),
        10 => "可转债上市".to_string(),
        11 => "扩缩股".to_string(),
        12 => "非流通股缩股".to_string(),
        13 => "送认购权证".to_string(),
        14 => "送认沽权证".to_string(),
        _ => format!("Unknown Category ({})", category),
    }
}