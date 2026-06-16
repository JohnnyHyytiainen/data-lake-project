# Session tracking notes for MVP v4 - Data quality and Pipeline Monitoring
*ongoing since 2026-06-13*

**Saturday 14/06-2026**
*Goals for today:*
- Start working on MvP v4
    - **Done**

- Choose between `soda-core-duckdb` vs `soda-core-spark` - document each pros / cons before choosing
    - **Done** - `soda-core-duckdb`

- Update self written docs briefly about soda-core, data contracts.
    - **Done**


- Start implementing soda core by:
    1) Update my `pyproject.toml`
        - **Done**

    2) Build my `Dockerfile.quality` - A lightweight image
        - **Done**

    3) Write my `quality/checks/silver_checks.yml`-contracts and practice soda CL syntaxt
        - **Done**

    4) write `quality/run_checks.py` - script and put together `DuckDB` + `Soda` 
        - **Done**

    5) Update my `github_lake_dag.py` - and add the new task.
        - **Done**

    6) Write docs and update ROADMAP.md with current status for everything
        - **Done**


--- 
**Tuesday 16/06-2026**
*Goals for today:*
- Update my circuit breaker to relax my data contracts a bit, running diagnostics I found 2 duplicate events. Will document it as a known limitation and clean it up in upcoming `Tech-debt`-sprint prior to MVP v6.
    - **Done**

- Start working on last step for MVP v4 (Logging check-results `data/quality_reports/` for historic traceability)
    *Ongoing*

- Write docs regarding `quality_repots` and what it is, and more importantly **WHY** its **NEEDED**
    - *ongoing*

    


    
