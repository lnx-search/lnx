#[macro_use]
extern crate log;

mod structures;
mod engine;
mod index;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
