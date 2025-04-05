use pest_derive::Parser;

#[derive(Debug, thiserror::Error)]
#[error("cannot parse query")]
pub struct ParseQueryError;

#[derive(Parser)]
#[grammar = "grammar/lql.pest"]
struct LqlParser;


/// Parse a LQL query into the core AST.
pub fn parse_query_string(query: &str) -> Result<(), ParseQueryError> {


    Ok(())
}

#[cfg(test)]
mod tests {
    use pest::Parser;
    use super::*;
        
    #[test]
    fn test_query_parse()  {
        
        let q = include_str!("../tests/queries/full_text_search.lql");
        match LqlParser::parse(Rule::query, q) {
            Err(e) => {
                println!("{e}");
                return;
            },
            Ok(p) => { dbg!(p); }
        }
    }
}