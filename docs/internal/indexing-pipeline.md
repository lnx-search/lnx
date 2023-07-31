# The Indexing Pipeline

The indexing pipeline is what actually processes our documents and feeds them into tantivy.

The pipeline is made of the following parts:

- An `IndexingActor` which pushes docs into the tantivy writer.
- A `TimingActor`which emits events for when the index should auto commit.
- A wrapping `IndexingPipeline` type that supervises the previously mentioned actors and provides
  access for interacting with the other actors.
- A `PipelineController` that manages running pipelines and restarts them if they crash.


