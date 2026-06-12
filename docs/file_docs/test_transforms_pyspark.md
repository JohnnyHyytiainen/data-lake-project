# Own docs for test_transforms.py script
When porting over from `Pandas` to `PySpark` which I run locally my `Github actions` `ci-pipeline` stopped working, which is obvious since my first testing was supposed to be used with pandas and tested all transformation logic when using `Pandas`.

But to dive a bit deeper into WHY my old testing logic crashed with my `PySpark`-migration is a good opportunity to understand the architecture behind it a bit more.

---

### Why the old tests crashed and what it teaches me
My old tests tested my `_is_valid()` and `_flatten()` functions. Two pure Python functions that took a `dict` as input and returned a `dict` or a `bool`. That was easy to test since it was pure logic without any side effects. When I ported to `PySpark`, those functions disappeared and all the logic ended up inline in `run_bronze_to_silver()`. 

What the function does is actually two fundamentally different things: 

- it handles I/O (read files, write files, checkpoint state) 
- and it handles transformation (filter, deduplicate, extract columns). 

These things are intertwined in the same function, and that’s exactly why its hard to test.

**A metaphor to put it into context:**   
Think of it like a kitchen in a restaurant. The chef is responsible for two things:  

- cooking the food and taking/delivering orders. If I want to check that the food tastes right, I dont need to involve the waitress, I would just taste the food directly. 

But if the chef and the waitress are the same person who never separate those roles, I can’t test the cooking without also simulating the entire order flow.

The solution is to extract the transformation logic into a pure function, a `_transform()` that takes a `Bronze DataFrame` and returns a `Silver DataFrame`, without a single file system call. It requires a minimal refactoring with a big impact.

The change in `bronze_to_silver.py` is to extract `_transform()`
