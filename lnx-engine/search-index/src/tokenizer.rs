use std::mem;

use deunicode::deunicode_char;
use tantivy::tokenizer::{BoxTokenStream, NgramTokenizer, SimpleTokenizer, Token, Tokenizer, TokenStream};

#[derive(Clone)]
pub struct CustomTokenizer;

impl Default for CustomTokenizer {
    fn default() -> Self {
        Self
    }
}

pub fn produce_ngrams(text: &str, min: usize, max: usize) -> Vec<Token> {
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
    let ngram = NgramTokenizer::all_ngrams(min, max);
    let mut stream = simple.token_stream(&characters);

    let mut ngrams = vec![];
    while let Some(token) = stream.next() {
        if token.text.len() < min {
            ngrams.push(token.clone());
            continue
        }

        if token.text.len() > max {
            ngrams.push(token.clone());
        }

        let mut inner_stream = ngram.token_stream(&token.text);
        while let Some(ngram) = inner_stream.next() {
            let token = Token {
                offset_from: token.offset_from + ngram.offset_from,
                offset_to: token.offset_from + ngram.offset_to,
                position: token.position,
                text: ngram.text.clone(),
                position_length: ngram.position_length,
            };
            ngrams.push(token);
        }
    }

    ngrams
}

pub struct SimpleTokenStream {
    tokens: Vec<Token>,
    pointer: usize,
}

impl Tokenizer for CustomTokenizer {
    fn token_stream<'a>(&self, text: &'a str) -> BoxTokenStream<'a> {
        let tokens = produce_ngrams(text, 4, 6);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ngram_system() {
        let text = "the truman supercalifragalistic show is";

        let simple = CustomTokenizer::default();
        let mut stream = simple.token_stream(&text);

        while let Some(token) = stream.next() {
            println!("{:?}", token);
        }
    }
}