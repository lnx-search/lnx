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
lnx is built to not re-invent the wheel, it stands on top of the [**tokio-rs**](https://tokio.rs) work-stealing runtime, [**axum**](https://github.com/tokio-rs/axum) a lightweight abstraction over [**hyper-rs**](https://github.com/hyperium/hyper) combined with the raw compute power of the [**tantivy search engine**](https://github.com/tantivy-search/tantivy).

Together this allows lnx to offer millisecond indexing on tens of thousands of document inserts at once (No more waiting around for things to get indexed!), Per index transactions and the ability to process searches like it's just another lookup on the hashtable 😲

### ✨ Features
lnx although very new offers a wide range of features thanks to the ecosystem it stands on.

- 🤓 **Complex Query Parser.**
- ❤️ **Typo tolerant fuzzy queries.**
- 🔥 **More-Like-This queries.**
- Order by fields.
- *Fast* indexing.
- *Fast* Searching.
- Several Options for fine grain performance tuning.
- Multiple storage backends available for testing and developing.
- Permissions based authorization access tokens.


### Performance
lnx can provide the ability to fine tune the system to your perticular use case. You can customise the async runtime threads. The concurrency thread pool, threads per reader and writer threads, all per index.

This gives you the ability to control in detail where your computing resources are going. Got a large dataset but lower amount of concurrent reads? Bump the reader threads in exchange for lower max concurrency.

This allows you to get some very nice results and tune your application to your needs:
<img src="https://github.com/ChillFish8/lnx/blob/master/benchmarks/diagrams/150-conn-results.png"/>

As a more detailed insight:

#### MeiliSearch
```
 INFO  lnxcli > starting benchmark system
 INFO  benchmark > starting runtime with 12 threads
 INFO  benchmark::meilisearch > MeiliSearch took 18.188s to process submitted documents
 INFO  benchmark              > Service ready! Beginning benchmark.
 INFO  benchmark              >      Concurrency @ 150 clients
 INFO  benchmark              >      Searching @ 50 sentences
 INFO  benchmark              >      Mode @ Standard
 INFO  benchmark::sampler     > General benchmark results:
 INFO  benchmark::sampler     >      Total Requests Sent: 7500
 INFO  benchmark::sampler     >      Average Requests/sec: 296.65
 INFO  benchmark::sampler     >      Average Latency: 505.654336ms
 INFO  benchmark::sampler     >      Max Latency: 725.2446ms
 INFO  benchmark::sampler     >      Min Latency: 10.085ms
 INFO  lnxcli                 > commands complete!
```

#### lnx
```
 INFO  lnxcli > starting benchmark system
 INFO  benchmark > starting runtime with 12 threads
 INFO  benchmark::lnx > lnx took 785.402ms to process submitted documents
 INFO  benchmark      > Service ready! Beginning benchmark.
 INFO  benchmark      >      Concurrency @ 150 clients
 INFO  benchmark      >      Searching @ 50 sentences
 INFO  benchmark      >      Mode @ Standard
 INFO  benchmark::sampler > General benchmark results:
 INFO  benchmark::sampler >      Total Requests Sent: 7500
 INFO  benchmark::sampler >      Average Requests/sec: 914.84
 INFO  benchmark::sampler >      Average Latency: 163.962587ms
 INFO  benchmark::sampler >      Max Latency: 668.0729ms
 INFO  benchmark::sampler >      Min Latency: 2.5241ms
 INFO  lnxcli             > commands complete!
```

### 💔 Limitations
As much as lnx provides a wide range of features, it can not do it all being such a young system. Naturally, it has some limitations:

- lnx is not distributed (yet) so this really does just scale vertically.
- Simple but not too simple, lnx can't offer the same level of ease of use compared to MeiliSearch due to its schema-full nature and wide range of tuning options. With more tuning comes more settings, unfortunately.
- Synonym support (yet)
- Metrics (yet)
