# lnx transforms

This crate defines all the transformation, type casting and validation logic that can be applied to documents as
they come through to be ingested.

The core `TransformPipeline` allows you to build transforms for a given object which can feed into the next transform.
Nested objects can simply build multiple pipelines for each nested level since `TransformPipeline` is itself a `Transform`.

### Supported Transforms

##### Type casting
The `TypeCastTransformer` allows you to define a type to try and cast to, the transform will then attempt to cast any incoming values to
this type sanely, this means it won't cast a negative value to a `u64` for example.

##### Type filter
Although type casting is useful, you may want to disable the casting for certain types and just flat out reject
them, to do this you can use the `TypeFilterTransformer` to allow or deny a set of enabled filter fields.

This also allows for specifying the type within arrays, i.e. `array<string>` can be done.

