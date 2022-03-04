use deunicode::deunicode_char;
use tantivy::tokenizer::{
    BoxTokenStream,
    SimpleTokenizer,
    Token,
    TokenStream,
    Tokenizer,
};

#[derive(Clone)]
pub struct SimpleUnicodeTokenizer;

impl Default for SimpleUnicodeTokenizer {
    fn default() -> Self {
        Self
    }
}

pub fn produce_tokens(text: &str) -> Vec<Token> {
    let mut characters = String::with_capacity(text.len());
    for char in text.chars() {
        if let Some(ascii) = deunicode_char(char) {
            if ascii.len() > 1 {
                characters.push(' ');
            }
            characters.push_str(&ascii.to_lowercase());
        }
    }

    let simple = SimpleTokenizer {};
    let mut stream = simple.token_stream(&characters);

    let mut tokens = vec![];
    while let Some(token) = stream.next() {
        tokens.push(token.clone());
    }

    tokens
}

pub struct SimpleTokenStream {
    tokens: Vec<Token>,
    pointer: usize,
}

impl Tokenizer for SimpleUnicodeTokenizer {
    fn token_stream<'a>(&self, text: &'a str) -> BoxTokenStream<'a> {
        let tokens = produce_tokens(text);

        BoxTokenStream::from(SimpleTokenStream { tokens, pointer: 0 })
    }
}

impl TokenStream for SimpleTokenStream {
    fn advance(&mut self) -> bool {
        if self.pointer < self.tokens.len() {
            self.pointer += 1;
            true
        } else {
            false
        }
    }

    fn token(&self) -> &Token {
        // safe because our pointer cannot go beyond bounds
        unsafe { self.tokens.get_unchecked(self.pointer - 1) }
    }

    fn token_mut(&mut self) -> &mut Token {
        // safe because our pointer cannot go beyond bounds
        unsafe { self.tokens.get_unchecked_mut(self.pointer - 1) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_and_compare(text: &str, expected: Vec<&str>) {
        let tokenizer = SimpleUnicodeTokenizer::default();
        let mut stream = tokenizer.token_stream(text);

        let mut tokens = vec![];
        while let Some(token) = stream.next() {
            tokens.push(token.text.to_string());
        }

        assert_eq!(tokens, expected);
    }

    #[test]
    fn test_plain_english() {
        let text = "hello world, I couldn't be more proud!";
        let tokens = vec!["hello", "world", "i", "couldn", "t", "be", "more", "proud"];
        parse_and_compare(text, tokens);
    }

    #[test]
    fn test_mixed() {
        let text = "Ôóű, 🦄☣ in 北亰";
        let tokens = vec!["oou", "unicorn", "biohazard", "in", "bei", "jing"];
        parse_and_compare(text, tokens);
    }

    #[test]
    fn test_accents() {
        let text = "étude";
        let tokens = vec!["etude"];
        parse_and_compare(text, tokens);
    }

    #[test]
    fn test_greek() {
        let text = "Æneid";
        let tokens = vec!["aeneid"];
        parse_and_compare(text, tokens);
    }

    #[test]
    fn test_other() {
        let text = "ᔕᓇᓇ";
        let tokens = vec!["sha", "na", "na"];
        parse_and_compare(text, tokens);
    }

    #[test]
    /// Note about this test:
    /// We don't really do much clever tokenizing here for CJK languages, this is
    /// mostly just testing the normalization rather than the tokenization ability.
    fn test_chinese_simplified() {
        let text = "你好，世界，我感到无比自豪！ ";
        let tokens = vec![
            "ni", "hao", "shi", "jie", "wo", "gan", "dao", "wu", "bi", "zi", "hao",
        ];
        parse_and_compare(text, tokens);
    }

    #[test]
    /// Note about this test:
    /// We don't really do much clever tokenizing here for CJK languages, this is
    /// mostly just testing the normalization rather than the tokenization ability.
    fn test_chinese_traditional() {
        let text = "你好，世界，我感到無比自豪！ ";
        let tokens = vec![
            "ni", "hao", "shi", "jie", "wo", "gan", "dao", "wu", "bi", "zi", "hao",
        ];
        parse_and_compare(text, tokens);
    }

    #[test]
    /// Note about this test:
    /// We don't really do much clever tokenizing here for CJK languages, this is
    /// mostly just testing the normalization rather than the tokenization ability.
    fn test_japanese() {
        let text = "Hello world、これ以上誇りに思うことはできません！ ";
        let tokens = vec![
            "hello", "world", "ko", "re", "yi", "shang", "kua", "ri", "ni", "si", "u",
            "ko", "to", "ha", "de", "ki", "ma", "sen",
        ];
        parse_and_compare(text, tokens);
    }

    #[test]
    /// Note about this test:
    /// We don't really do much clever tokenizing here for CJK languages, this is
    /// mostly just testing the normalization rather than the tokenization ability.
    fn test_korean() {
        let text = "안녕하세요 세상, 이보다 더 자랑스러울 수 없습니다! ";
        let tokens = vec![
            "an", "nyeong", "ha", "se", "yo", "se", "sang", "i", "bo", "da", "deo",
            "ja", "rang", "seu", "reo", "ul", "su", "eobs", "seub", "ni", "da",
        ];
        parse_and_compare(text, tokens);
    }
}
