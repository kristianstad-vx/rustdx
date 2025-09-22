use argh::FromArgs;
use eyre::Result;

/// 例子：`rustdx gbbq /path/to/gbbq/file -o gbbq_output.csv`
/// 或者：`rustdx gbbq /path/to/gbbq/file -o clickhouse -t rustdx.gbbq`
#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "gbbq")]
pub struct GbbqCmd {
    /// 必选。指定 GBBQ 文件路径。
    #[argh(positional)]
    pub gbbq_file: std::path::PathBuf,

    /// 可选。解析后的输出方式。默认值为 gbbq_output.csv。
    /// 支持：csv文件路径 | clickhouse | mongodb
    #[argh(option, short = 'o', default = "String::from(\"gbbq_output.csv\")")]
    pub output: String,

    /// 可选。指定时，表示保存 csv 文件。只针对非 csv output 有效。
    #[argh(switch, short = 'k', long = "keep-csv")]
    pub keep_csv: bool,

    /// 可选。指定表名称，默认为 `rustdx.gbbq`。
    #[argh(option, short = 't', default = "String::from(\"rustdx.gbbq\")")]
    pub table: String,

    /// 可选。过滤特定类别的记录。例如：-c 1 只导出除权除息记录。
    #[argh(option, short = 'c')]
    pub category: Option<u8>,

    /// 可选。过滤特定股票代码。例如：-s 000001,000002
    #[argh(option, short = 's')]
    pub stocks: Option<String>,

    /// 可选。过滤日期范围。格式：YYYYMMDD-YYYYMMDD 例如：-d 20200101-20231231
    #[argh(option, short = 'd')]
    pub date_range: Option<String>,

    /// 可选。显示详细的使用说明。
    #[argh(option, short = 'h')]
    description: Vec<String>,
}

impl GbbqCmd {
    pub fn run(&self) -> Result<()> {
        match self.output.as_str() {
            "clickhouse" => self.run_clickhouse(),
            x if x.ends_with("csv") => self.run_csv(),
            _ => {
                eprintln!("Unsupported output format: {}", self.output);
                eprintln!("Supported formats: csv file path, 'clickhouse', 'mongodb'");
                Ok(())
            }
        }
    }

    pub fn run_csv(&self) -> Result<()> {
        crate::io::run_gbbq_csv(self)
    }

    /// clickhouse-client --query "INSERT INTO table FORMAT CSVWithNames" < output.csv
    pub fn run_clickhouse(&self) -> Result<()> {
        crate::io::setup_gbbq_clickhouse(&self.table)?;
        self.run_csv()?;
        crate::io::insert_clickhouse(&self.output, &self.table, self.keep_csv)
    }

    pub fn help_info(&self) -> &Self {
        for arg in &self.description {
            match arg.as_str() {
                "output" | "o" => println!("{GBBQ_OUTPUT}"),
                "category" | "c" => println!("{GBBQ_CATEGORY}"),
                "stocks" | "s" => println!("{GBBQ_STOCKS}"),
                "date" | "d" => println!("{GBBQ_DATE}"),
                _ => println!(
                    "请查询以下参数: output category stocks date 或者它们的简写 o c s d;\n\
                     使用 `-h o -h c` 的形式查询多个参数的使用方法"
                ),
            }
        }
        self
    }

    /// 解析股票代码列表
    pub fn parse_stocks(&self) -> Option<Vec<String>> {
        self.stocks.as_ref().map(|s| {
            s.split(',')
                .map(|code| code.trim().to_string())
                .collect()
        })
    }

    /// 解析日期范围
    pub fn parse_date_range(&self) -> Option<(u32, u32)> {
        self.date_range.as_ref().and_then(|range| {
            let parts: Vec<&str> = range.split('-').collect();
            if parts.len() == 2 {
                let start = parts[0].parse().ok()?;
                let end = parts[1].parse().ok()?;
                Some((start, end))
            } else {
                None
            }
        })
    }
}

#[rustfmt::skip]
const GBBQ_OUTPUT: &str = "--output 或 -o :
解析后的输出方式：
`-o csv_path.csv` 保存成 csv 格式，默认值为 gbbq_output.csv
`-o clickhouse` 保存成 csv 格式，并把 csv 的数据插入到 clickhouse 数据库
`-o mongodb` 保存成 csv 格式，并把 csv 的数据插入到 mongodb 数据库

注意：
1. 成功插入到 clickhouse 或 mongodb 数据库之后，默认会删除掉解析的 csv 文件。
   如果需要保存这个文件，使用 `-k` 参数。
2. clickhouse 数据库必须先建表再插入数据，因此本工具会提前建表。
";

#[rustfmt::skip]
const GBBQ_CATEGORY: &str = "--category 或 -c :
过滤特定类别的记录：
1  - 除权除息 (Ex-rights & Dividends)
2  - 送配股上市 (Bonus/Rights Shares Listing)
3  - 非流通股上市 (Non-tradable Shares Listing)
4  - 未知股本变动 (Unknown Capital Change)
5  - 股本变化 (Capital Change)
6  - 增发新股 (New Share Issuance)
7  - 股份回购 (Share Buyback)
8  - 增发新股上市 (New Share Issuance Listing)
9  - 转配股上市 (Transferred Shares Listing)
10 - 可转债上市 (Convertible Bonds Listing)
11 - 扩缩股 (Share Expansion/Contraction)
12 - 非流通股缩股 (Non-tradable Share Contraction)
13 - 送认购权证 (Warrant Issuance)
14 - 送认沽权证 (Put Warrant Issuance)

例子：`-c 1` 只导出除权除息记录
";

#[rustfmt::skip]
const GBBQ_STOCKS: &str = "--stocks 或 -s :
过滤特定股票代码，使用逗号分隔:
例子: `-s 000001,000002,600000` 只导出这三只股票的记录
";

#[rustfmt::skip]
const GBBQ_DATE: &str = "--date-range 或 -d :
过滤日期范围，格式: YYYYMMDD-YYYYMMDD
例子：`-d 20200101-20231231` 只导出2020年到2023年的记录
";