
#[repr(C)]
/// A signal that indicates what format a buffer is in.
pub enum ParseFormat {
    /// The buffer is in the JSON format as a single object.
    Json,
    /// The buffer is in the NDJSON format which contains potentially
    /// multiple documents.
    Ndjson,
    /// The buffer is in the MSGPACK format which contains potentially
    /// multiple documents _or_ a single object map.
    Msgpack,
}

#[repr(C)]
/// The format to serialize the document into.
pub enum SerializeFormat {
    /// Serialize the doc into a JSON object.
    Json,
    /// Serialize the doc into a msgpack object.
    Msgpack,
}