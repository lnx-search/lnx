<p align="center">
    <h1>lnx v0.10.0-alpha</h1>
</p>

<p align="center">
  <img width="30%" src="https://user-images.githubusercontent.com/57491488/156235904-5c0f956f-1bd7-4b7e-8cd0-fd344db7e632.png" alt="lnx Logo">
</p>

#
<p align="center">
  <a href="https://github.com/lnx-search/lnx/stargazers"><img src="https://img.shields.io/github/stars/lnx-search/lnx"/></a>
  <a href="hhttps://github.com/lnx-search/lnx/issues"><img src="https://img.shields.io/github/issues/lnx-search/lnx"/></a>
  <a href="https://github.com/lnx-search/lnx/blob/master/LICENSE"><img src="https://img.shields.io/github/license/lnx-search/lnx"/></a>
  <a href="https://docs.lnx.rs"><img src="https://img.shields.io/badge/Docs-alive-sucess"/></a>
</p>
<p align="center"><a href="https://lnx.rs">✨ Feature Rich | ⚡ Insanely Fast</a></p>
<p align="center">An ultra-fast, adaptable deployment of the tantivy search engine via REST.</p>
<p align="center">
  Join our community for support, updates and more:
</p>
<p align="center">
  <a href="https://discord.gg/hPr7BQGgb4"><img src="https://img.shields.io/badge/Discord-7289DA?style=for-the-badge&logo=discord&logoColor=white"/></a>  
</p>

### 🌟 Standing On The Shoulders of Giants
lnx is built to not re-invent the wheel, it stands on top of the [**tokio-rs**](https://tokio.rs) work-stealing runtime, [**hyper**](https://hyper.rs/) web framework combined with the raw compute power of the [**tantivy search engine**](https://github.com/tantivy-search/tantivy).

Together this allows lnx to offer millisecond indexing on tens of thousands of document inserts at once (No more waiting around for things to get indexed!), Per index transactions and the ability to process searches like it's just another lookup on the hashtable 😲

### ✨ Features
lnx although very new offers a wide range of features thanks to the ecosystem it stands on.

- 🤓 **Complex Query Parser.**
- ❤️ **Typo tolerant fuzzy queries.**
- ⚡️ **Typo tolerant fast-fuzzy queries. (pre-computed spell correction)**
- 🔥 **More-Like-This queries.**
- Order by fields.
- *Fast* indexing.
- *Fast* Searching.
- Several Options for fine grain performance tuning.
- Multiple storage backends available for testing and developing.
- Permissions based authorization access tokens.

<p align="center">
  <img src="https://i.imgur.com/QovtWlc.gif" alt="Demo video"/>
</p>

*Here you can see lnx doing search as you type on a 27 million document dataset coming in at reasonable 18GB once indexed, ran on my i7-8700k using ~3GB of RAM with our fast-fuzzy system*
Got a bigger dataset for us to try? Open an issue!

### Performance
lnx can provide the ability to fine tune the system to your particular use case. You can customise the async runtime threads. The concurrency thread pool, threads per reader and writer threads, all per index.

This gives you the ability to control in detail where your computing resources are going. Got a large dataset but lower amount of concurrent reads? Bump the reader 
threads in exchange for lower max concurrency.

The bellow figures were taken by our `lnx-cli` on the small `movies.json` dataset, we didn't try any higher as Meilisearch takes an incredibly long time to index millions of docs although the new Meilisearch engine has improved this somewhat.

<p align="center">
<img width="45%" src="https://user-images.githubusercontent.com/57491488/149216271-6d30eae4-bb42-4121-a734-9fbd1bac2902.png"/>
<img width="45%" src="https://user-images.githubusercontent.com/57491488/149216285-705d4700-e10f-4ffe-a0f2-2fb325ba3004.png"/>
</p>

### 💔 Limitations
As much as lnx provides a wide range of features, it can not do it all being such a young system. Naturally, it has some limitations:

- lnx is not distributed (yet) so this really does just scale vertically.
- Simple but not too simple, lnx can't offer the same level of ease of use compared to MeiliSearch due to its schema-full nature and wide range of tuning options. With more tuning comes more settings, unfortunately.
- Metrics (yet)

### Local Development

Setup and usage is overall, as simple as cloning the repo and running `cargo build` or `cargo run` but on linux there
is an additional caveat due to the DIRECT_IO implementation requirements:

lnx requires a kernel with a recent enough io_uring support, at least current enough to run discovery probes. The minimum version at this time is 5.8.

Please also note lnx requires at least 512 KiB of locked memory for io_uring to work. You can increase the memlock resource limit (rlimit) as follows:
```
$ vi /etc/security/limits.conf
*    hard    memlock        512
*    soft    memlock        512
```

> Please note that 512 KiB is the minimum needed to spawn a single executor. 
> Spawning multiple executors (multi-core runtime) may require you to raise the limit accordingly.

To make the new limits effective, you need to log in to the machine again. You can verify that the limits are updated by running the following:

```shell
$ ulimit -l
512
```