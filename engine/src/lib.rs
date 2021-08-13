#[macro_use]
extern crate log;

mod engine;
mod index;
mod structures;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
