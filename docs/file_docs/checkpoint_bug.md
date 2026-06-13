# Discovery of a bug in checkpoint file.
Since Im using file state tracking to see already ingested and read files to skip already processed files and only process new files I implemented a `checkpoint`-file, AKA `file state tracking`. Since this is a local project and I have no overhead costs this was the most precise way to keep track on files and optimise my pipeline.

---

**However, after a refactor and breaking out my transformation logic from my main function in bronze_to_silver.py** I now discovered a funny bug when running my DAGs. The issue is this:

```json
"year=2025\\month=12\\day=05\\bootstrap-20260411_182224_554844.parquet",
"year=2025/month=12/day=12/bootstrap-20260410_213921_018423.parquet",
"year=2026/month=01/day=03/bootstrap-20260410_112542_778965.parquet",
"year=2026/month=01/day=09/bootstrap-20260410_113937_033903.parquet",
"year=2026/month=03/day=08/bootstrap-20260405_133921_153013.parquet",
"year=2026\\month=05\\day=13\\bootstrap-20260518_103830_671405.parquet",
"year=2025/month=11/day=28/bootstrap-20260412_171934_448008.parquet",
"year=2026/month=03/day=02/bootstrap-20260405_133209_771655.parquet",
"year=2025\\month=08\\day=02\\bootstrap-20260420_185130_499154.parquet",
"year=2026\\month=04\\day=04\\bootstrap-20260405_141718_023602.parquet",
"year=2026\\month=04\\day=02\\bootstrap-20260403_212011_952573.parquet",
"year=2025/month=11/day=23/bootstrap-20260412_170947_914148.parquet"
```
When it should be like this:
```json
"year=2025/month=12/day=12/bootstrap-20260410_213921_018423.parquet",
"year=2026/month=01/day=03/bootstrap-20260410_112542_778965.parquet",
"year=2026/month=01/day=09/bootstrap-20260410_113937_033903.parquet",
```

- The reason for this is that `str(Path(.....))` acts differently on Windows machines and Linux machines. The current row in my `_save_checkpoint():`-function:

```python
relative_paths = [str(Path(f).relative_to(BRONZE_DIR)) for f in processed_files]
```

- `str(Path(....))` is `OS`-specific. On windows it produces backslashes but on a Linux machine it does forward slashes. Prior to my refactoring i only ran the checkpoint logic in `Docker(LINUX)`. The consequence in `Docker`: is that `BRONZE_DIR / "year=2025\\month=12\\day=05\\fil.parquet"` interprets the entire string as ONE SINGLE FOLDER-NAME on Linux since backslashes isnt a path delimiter(sökvägsavgränsare). It never finds the file and each new entry with backslashes gets treated as a new run.

---

### The fix - Two lines, one concept.
The solution should be: `Path.as_posix()` since it always returns forward slashes regardless on what OS it runs on. Where to implement the fix?

1) - In my `_save_checkpoint():`-function in `bronze_to_silver.py`-script I will change `str(Path(......))` with `as_posix():`
2) - In my `_load_checkpoint():`-function, i will normalise backslashes at reading so that existing entries that are wrong in my checkpoint file gets handled correctly.

- From this in 1): 

```python
relative_paths = [str(Path(f).relative_to(BRONZE_DIR)) for f in processed_files]
```

- To this in 1):
```python
    relative_paths = [
        Path(f).relative_to(BRONZE_DIR).as_posix() for f in processed_files
    ]
```

- Form this in 2):

```python
    with open(BRONZE_SILVER_CHECKPOINT, "r") as f:
        data = json.load(f)
        relative_paths = set(data.get("processed_files", []))
        processed = {str(BRONZE_DIR / rel) for rel in relative_paths}
        last_run = data.get("last_run", "unknown")
        logger.info(
            f"Checkpoint loaded | {len(processed)} files already processed | last_run={last_run}"
        )
        return processed
```

- To this in 2):
```python
    with open(BRONZE_SILVER_CHECKPOINT, "r") as f:
        data = json.load(f)
        relative_paths = set(data.get("processed_files", []))
        processed = {str(BRONZE_DIR / Path(rel.replace("\\", "/"))) for rel in relative_paths}
        last_run = data.get("last_run", "unknown")
        logger.info(
            f"Checkpoint loaded | {len(processed)} files already processed | last_run={last_run}"
        )
        return processed
```

## Then to rest if my logic is sound:
- To test the logic, i need to run this command `rm data/checkpoints/bronze_to_silver.json` first to remove my existing checkpoint file and then trigger my DAGs once again for a cold run to see if the refactor i implemented is valid and sticks