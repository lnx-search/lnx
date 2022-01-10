<p align="center">
  <img width="20%" src="https://github.com/ChillFish8/lnx/blob/master/assets/logo.png" alt="Lust Logo">
</p>

#
<p align="center">
  <a href="https://github.com/ChillFish8/lnx/stargazers"><img src="https://img.shields.io/github/stars/ChillFish8/lnx"/></a>
  <a href="hhttps://github.com/ChillFish8/lnx/issues"><img src="https://img.shields.io/github/issues/ChillFish8/lnx"/></a>
  <a href="https://github.com/ChillFish8/lnx/blob/master/LICENSE"><img src="https://img.shields.io/github/license/ChillFish8/lnx"/></a>
  <a href="https://docs.lnx.rs"><img src="https://img.shields.io/badge/Docs-alive-sucess"/></a>
</p>
<p align="center"><a href="https://lnx.rs">✨ Feature Rich | ⚡ Insanely Fast</a></p>
<p align="center">An ultra-fast, adaptable deployment of the tantivy search engine via REST.</p>

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

*Here you can see lnx doing search as you type on a 27 million document dataset coming in at reasonable 18GB once indexed, ran on my i7-8700k using ~4GB of RAM with our fast-fuzzy system*
Got a bigger dataset for us to try? Open an issue!

### Performance
lnx can provide the ability to fine tune the system to your particular use case. You can customise the async runtime threads. The concurrency thread pool, threads per reader and writer threads, all per index.

This gives you the ability to control in detail where your computing resources are going. Got a large dataset but lower amount of concurrent reads? Bump the reader 
threads in exchange for lower max concurrency.

The bellow figures were taken by our `lnx-cli` on the small `movies.json` dataset, we didn't try any higher as Meilisearch takes an incredibly long time to index millions of docs although the new Meilisearch engine has improved this somewhat.

#### MeiliSearch
```
 INFO  lnxcli > starting benchmark system
 INFO  benchmark > starting runtime with 12 threads
 INFO  benchmark::meilisearch > MeiliSearch took 17.177s to process submitted documents
 INFO  benchmark              > Service ready! Beginning benchmark.
 INFO  benchmark              >      Concurrency @ 200 clients
 INFO  benchmark              >      Searching @ 10 sentences
 INFO  benchmark              >      Mode @ Typing
 INFO  benchmark::sampler     > General benchmark results:
 INFO  benchmark::sampler     >      Total Requests Sent: 67633
 INFO  benchmark::sampler     >      Average Requests/sec: 532.88
 INFO  benchmark::sampler     >      Average Latency: 374.254422ms
 INFO  benchmark::sampler     >      Max Latency: 3.924168s
 INFO  benchmark::sampler     >      Min Latency: 1.9643ms
 WARN  benchmark::sampler     >      Got status 500: 767
 INFO  lnxcli                 > commands complete!
```

#### lnx (fast-fuzzy search)
```
INFO  lnxcli > starting benchmark system
 INFO  benchmark > starting runtime with 12 threads
 INFO  benchmark::lnx > lnx took 1.3684143s to process submitted documents
 INFO  benchmark      > Service ready! Beginning benchmark.
 INFO  benchmark      >      Concurrency @ 200 clients
 INFO  benchmark      >      Searching @ 10 sentences
 INFO  benchmark      >      Mode @ Typing
 INFO  benchmark::sampler > General benchmark results:
 INFO  benchmark::sampler >      Total Requests Sent: 68400
 INFO  benchmark::sampler >      Average Requests/sec: 5237.24
 INFO  benchmark::sampler >      Average Latency: 38.18735ms
 INFO  benchmark::sampler >      Max Latency: 3.0432477s
 INFO  benchmark::sampler >      Min Latency: 758µs
 INFO  lnxcli             > commands complete!
```




### 💔 Limitations
As much as lnx provides a wide range of features, it can not do it all being such a young system. Naturally, it has some limitations:

- lnx is not distributed (yet) so this really does just scale vertically.
- Simple but not too simple, lnx can't offer the same level of ease of use compared to MeiliSearch due to its schema-full nature and wide range of tuning options. With more tuning comes more settings, unfortunately.
- Synonym support (yet)
- Metrics (yet)
