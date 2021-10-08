use serde::Serialize;
use thruster::BasicContext as Ctx;

#[derive(Serialize)]
pub struct Response<'a, T: Serialize + ?Sized> {
    status: u16,
    data: &'a T
}

pub fn json_response<T: Serialize + ?Sized>(mut ctx: Ctx, status: u16, body: &T) -> Ctx {
    ctx.set_status(status as u32);
    ctx.json(Response {
        status,
        data: body,
    });

    ctx
}