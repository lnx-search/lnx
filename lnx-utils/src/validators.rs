pub trait Validator {
    type Error;

    fn validate(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    
    fn validate_with_schema(
        &mut self, 
        _schema: tantivy::schema::Schema
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}