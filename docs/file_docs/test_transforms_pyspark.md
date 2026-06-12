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

---

### Inline logic:
What does `inline logic` mean?

- Inline refers to a computing term where code or data is inserted directly into its appropriate place within a larger block of code, rather than being called from a separate location. It allows for more efficient execution and can often improve performances.


### How to separate transformation logic and break that logic out from my main function in bronze_to_silver.py
To break out my transformation logic from my main function `def run_bronze_to_silver()`, I have to create another function that I will name `def _transform(df_bronze: "DataFrame") -> "DataFrame":` which handles pure transformation logic. It takes a bronze dataframe and writes a silver dataframe. No file I/O logic, no checkpoint logic, only pure transformation logic. By doing this refactor i keep to the separation of concerns principle in practice since `_transform()` function is deterministic and easy to test the logic on. While doing this it clears up my main `run_bronze_to_silver()`-function and lets that function handle `I/O`, `File state tracking(checkpoint)` and can that way test my main function via integration.

