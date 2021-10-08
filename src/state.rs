use std::collections::HashMap;

use bytes::Bytes;
use engine::{Engine, StorageBackend};
use serde::Serialize;
use thruster::middleware::cookies::{Cookie, CookieOptions, HasCookies};
use thruster::middleware::query_params::HasQueryParams;
use thruster::{BasicContext, Context, Request, Response};

#[allow(unused)]
#[derive(Clone)]
pub struct State {
    engine: Engine,
    storage: StorageBackend,
}

impl State {
    pub fn new(engine: Engine, storage: StorageBackend) -> Self {
        Self { engine, storage }
    }
}

pub type Ctx = LnxContext;

#[derive(Clone)]
pub struct LnxContext {
    inner: BasicContext,
    pub state: State,
}

#[allow(unused)]
impl LnxContext {
    pub fn new(ctx: BasicContext, state: State) -> Self {
        Self { inner: ctx, state }
    }
    ///
    /// Set the body as a string
    ///
    pub fn body(&mut self, body_string: &str) {
        self.inner.body(body_string)
    }

    ///
    /// Set Generic Serialize as body and sets header Content-Type to application/json
    ///
    pub fn json<T: Serialize>(&mut self, body: T) {
        self.inner.json(body)
    }

    ///
    /// Set the response status code
    ///
    pub fn set_status(&mut self, code: u32) -> &mut LnxContext {
        self.inner.set_status(code);

        self
    }

    pub fn get_body(&self) -> String {
        self.inner.get_body()
    }

    ///
    /// Set the response status code
    ///
    pub fn status(&mut self, code: u32) {
        self.inner.status(code)
    }

    ///
    /// Set the response `Content-Type`. A shortcode for
    ///
    /// ```ignore
    /// ctx.set("Content-Type", "some-val");
    /// ```
    ///
    pub fn content_type(&mut self, c_type: &str) {
        self.inner.content_type(c_type)
    }

    ///
    /// Set up a redirect, will default to 302, but can be changed after
    /// the fact.
    ///
    /// ```ignore
    /// ctx.set("Location", "/some-path");
    /// ctx.status(302);
    /// ```
    ///
    pub fn redirect(&mut self, destination: &str) {
        self.inner.redirect(destination)
    }

    ///
    /// Sets a cookie on the response
    ///
    pub fn cookie(&mut self, name: &str, value: &str, options: &CookieOptions) {
        self.inner.cookie(name, value, options)
    }
}

impl Context for LnxContext {
    type Response = Response;

    fn get_response(self) -> Self::Response {
        self.inner.get_response()
    }

    fn set_body(&mut self, body: Vec<u8>) {
        self.inner.set_body(body)
    }

    fn set_body_bytes(&mut self, bytes: Bytes) {
        self.inner.set_body_bytes(bytes)
    }

    fn route(&self) -> &str {
        self.inner.route()
    }

    fn set(&mut self, key: &str, value: &str) {
        self.inner.set(key, value)
    }

    fn remove(&mut self, key: &str) {
        self.inner.remove(key)
    }
}

impl HasQueryParams for LnxContext {
    fn set_query_params(&mut self, query_params: HashMap<String, String>) {
        self.inner.set_query_params(query_params)
    }
}

impl HasCookies for LnxContext {
    fn set_cookies(&mut self, cookies: Vec<Cookie>) {
        self.inner.set_cookies(cookies)
    }

    fn get_cookies(&self) -> Vec<String> {
        self.inner.get_cookies()
    }

    fn get_header(&self, key: &str) -> Vec<String> {
        self.inner.get_header(key)
    }
}

pub fn generate_context(request: Request, state: &State, path: &str) -> Ctx {
    let ctx = thruster::context::basic_context::generate_context(request, state, path);

    LnxContext::new(ctx, state.clone())
}
