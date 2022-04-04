// use std::collections::BTreeMap;
// use scylla::BufMut;
// use scylla::frame::value::Value;
// use scylla::frame::value::ValueTooBig;
// use serde_json::Number;
// use lnx_storage::types::Timestamp;
// use lnx_utils::ToJSON;

// pub struct ScyllaSafeValue(pub lnx_common::types::Value);
//
// impl scylla::frame::value::Value for ScyllaSafeValue {
//     fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
//         match &self.0 {
//             lnx_common::types::Value::I64(v) => v.serialize(buf),
//             lnx_common::types::Value::U64(v) => (*v as i64).serialize(buf),
//             lnx_common::types::Value::F64(v) => v.serialize(buf),
//             lnx_common::types::Value::DateTime(v) => Timestamp::from(v.timestamp_millis()).serialize(buf),
//             lnx_common::types::Value::Text(v) => v.serialize(buf),
//             lnx_common::types::Value::Bytes(v) => v.serialize(buf),
//             lnx_common::types::Value::Json(v) => todo!(),
//         }
//     }
// }

// fn serialize_mapping(
//     buf: &mut Vec<u8>,
//     v: &serde_json::Map<String, serde_json::Value>,
// ) -> Result<(), ValueTooBig> {
//
//     let bytes_num_pos: usize = buf.len();
//     buf.put_i32(0);
//
//     buf.put_i32(v.len().try_into().map_err(|_| ValueTooBig)?);
//     for (key, value) in v.iter() {
//         key.serialize(buf)?;
//         serialize_serde_value(buf, value)?;
//     }
//
//     let written_bytes: usize = buf.len() - bytes_num_pos - 4;
//     let written_bytes_i32: i32 = written_bytes.try_into().map_err(|_| ValueTooBig)?;
//     buf[bytes_num_pos..(bytes_num_pos + 4)].copy_from_slice(&written_bytes_i32.to_be_bytes());
//
//     Ok(())
// }

//
// fn serialize_serde_value(
//     buf: &mut Vec<u8>,
//     v: &serde_json::Value,
// ) -> Result<(), ValueTooBig> {
//     match v {
//         serde_json::Value::Null => Ok(buf.put_i32(-1)),
//         serde_json::Value::Bool(v) => v.serialize(buf),
//         serde_json::Value::Number(n) => {
//             if let Some(n) = n.as_i64() {
//                 return n.serialize(buf)
//             }
//
//             if let Some(n) = n.as_f64() {
//                 return n.serialize(buf)
//             }
//
//             Ok(buf.put_i32(-1))  // No real way to represent that.
//         },
//         serde_json::Value::String(v) => v.serialize(buf),
//         serde_json::Value::Array(values) => {
//             for inner in values {
//                 match inner {
//                     serde_json::Value::Null => Ok(buf.put_i32(-1)),
//                     serde_json::Value::Bool(v) => v.serialize(buf),
//                     serde_json::Value::Number(n) => {
//                         if let Some(n) = n.as_i64() {
//                             return n.serialize(buf)
//                         }
//
//                         if let Some(n) = n.as_f64() {
//                             return n.serialize(buf)
//                         }
//
//                         Ok(buf.put_i32(-1))  // No real way to represent that.
//                     },
//                     serde_json::Value::String(v) => v.serialize(buf),
//                 }
//             }
//         }
//         serde_json::Value::Object(v) => {
//             if let Ok(v) = v.to_json() {
//                 v.serialize(buf)
//             } else {
//                 Ok(buf.put_i32(-1))  // No real way to represent that.
//             }
//         }
//     }
// }