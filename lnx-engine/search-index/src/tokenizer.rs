use deunicode::deunicode_char;
use tantivy::tokenizer::{BoxTokenStream, SimpleTokenizer, Token, Tokenizer, TokenStream};

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
            characters.extend(ascii.to_lowercase().chars())
        }
    }

    let simple = SimpleTokenizer{};
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

        BoxTokenStream::from(SimpleTokenStream {
            tokens,
            pointer: 0,
        })
    }
}

impl TokenStream for SimpleTokenStream  {
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
