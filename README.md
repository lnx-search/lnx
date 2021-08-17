<p align="center">
  <img width="20%" src="https://github.com/ChillFish8/lnx/blob/master/assets/logo.png" alt="Lust Logo">
</p>

#
<p align="center">
  <a href="https://github.com/ChillFish8/lnx/stargazers"><img src="https://img.shields.io/github/stars/ChillFish8/lnx"/></a>
  <a href="hhttps://github.com/ChillFish8/lnx/issues"><img src="https://img.shields.io/github/issues/ChillFish8/lnx"/></a>
  <a href="https://github.com/ChillFish8/lnx/blob/master/LICENSE"><img src="https://img.shields.io/github/license/ChillFish8/lnx"/></a>
  <a href="https://book.lnx.rs"><img src="https://img.shields.io/badge/Book-alive-sucess"/></a>
</p>
<p align="center"><a href="https://book.lnx.rs">✨ Feature Rich | ⚡ Insanely Fast</a></p>
<p align="center">An ultra-fast, adaptable deployment of the tantivy search engine via REST.</p>

### 🌟 Standing On The Shoulders of Giants
lnx is built to no re-invent the wheel, it stands ontop of the [**tokio-rs**](https://tokio.rs) work-stealing runtime, [**axum**](https://github.com/tokio-rs/axum) a lightweight abstraction over [**hyper-rs**](https://github.com/hyperium/hyper) combined with the raw compute power of the [**tantivy search engine**](https://github.com/tantivy-search/tantivy).

Together this allows lnx to offer millisecond indexing on tens of thousands of document inserts at once (No more waiting around for things to get indexed!), Per index transactions and the ability to process searches like it's just another lookup on the hashtable 😲

### ✨ Features
lnx although very new offers a wide range of features thanks to the ecosystem it stands on.

- 🤓 **Complex Query Parser.**
- ❤️ **Typo tollerant fuzzy queries.**
- 🔥 **More-Like-This queries.**
- Order by fields.
- *Fast* indexing.
- *Fast* Searching.
- Several Options for fine grain performance tuning.
- Multiple storage backends available for testing and developing.


