use std::str::FromStr;

/// The two benchmarking targets.
///
/// This was designed to compare MeiliSearch and lnx.
#[derive(Debug)]
pub enum BenchTarget {
    MeiliSearch,
    Lnx,
}

impl FromStr for BenchTarget {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "meilisearch" => Ok(Self::MeiliSearch),
            "lnx" => Ok(Self::Lnx),
            other => Err(format!(
                "unknown target type got {:?}, expected either 'meilisearch' or 'lnx'",
                other,
            ))
        }
    }
}

/// The benchmark type to run, this allows us to have several
/// modes and areas to test.
#[derive(Debug)]
pub enum BenchMode {
    /// Simulates typing our a sentence / word a character at a time.
    Typing,

    /// Simulates a standard searching a set of words.
    Standard,
}

impl FromStr for BenchMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "typing" => Ok(Self::Typing),
            "standard" => Ok(Self::Standard),
            other => Err(format!(
                "unknown benchmark type got {:?}, expected either 'typing' or 'standard'",
                other,
            ))
        }
    }
}

pub struct Context {
    pub address: String,
    pub data_file: String,
    pub concurrency: usize,
    pub target: BenchTarget,
    pub mode: BenchMode,
    pub threads: usize,
}

pub fn run(ctx: Context) -> anyhow::Result<()> {
    Ok(())
}
