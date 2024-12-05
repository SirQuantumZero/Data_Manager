# Project Structure

Generated on: 2024-12-04 22:35:33

## Project Statistics
- Total Files: 55
- Total Lines: 6467
- Python Lines: 5345
- Total Functions: 235
- Total Classes: 46
- Last Updated: 2024-12-04 22:35:33.591699

### Languages
- Python: 45 files
- SQL: 3 files
- Markdown: 2 files

## Directory Structure
```
./
    __init__.py                                   # 0 lines, 0 functions, 0 classes
    setup.py                                      # 27 lines, 0 functions, 0 classes
    db/
        backups/
            backup_20241202_233709.sql            # 0 lines
        migrations/
            schema.sql                            # 522 lines
            test_schema.sql                       # 576 lines
    docs/
        PROJECT_STRUCTURE.md                      # 0 lines
        README.md                                 # 24 lines
    src/
        data_fetcher.py                           # 378 lines, 14 functions, 1 classes
        data_manager.py                           # 326 lines, 4 functions, 3 classes
        database_client.py                        # 60 lines, 5 functions, 1 classes
        database_config.py                        # 25 lines, 1 functions, 2 classes
        database_manager.py                       # 896 lines, 48 functions, 8 classes
        exceptions.py                             # 8 lines, 0 functions, 2 classes
        models.py                                 # 152 lines, 5 functions, 8 classes
        api/
            __init__.py                           # 0 lines, 0 functions, 0 classes
            endpoints.py                          # 116 lines, 1 functions, 3 classes
        cache/
            __init__.py                           # 0 lines, 0 functions, 0 classes
            memory_cache.py                       # 139 lines, 9 functions, 5 classes
        fetch_modules/
            __init__.py                           # 0 lines, 0 functions, 0 classes
            base/
                __init__.py                       # 0 lines, 0 functions, 0 classes
                base_data_source_.py              # 38 lines, 0 functions, 1 classes
            mock/
                __init__.py                       # 0 lines, 0 functions, 0 classes
                mock_api.py                       # 57 lines, 3 functions, 1 classes
                mock_data_source.py               # 139 lines, 8 functions, 3 classes
            polygon/
                __init__.py                       # 4 lines, 0 functions, 0 classes
                polygon_client.py                 # 33 lines, 2 functions, 1 classes
                polygon_data_source.py            # 88 lines, 1 functions, 1 classes
                polygon_fetch_data.py             # 54 lines, 1 functions, 1 classes
        managers/
            __init__.py                           # 0 lines, 0 functions, 0 classes
            market_data.py                        # 195 lines, 1 functions, 1 classes
    tests/
        conftest.py                               # 20 lines, 2 functions, 0 classes
        test_data_manager.py                      # 39 lines, 1 functions, 0 classes
        test_database_manager.py                  # 103 lines, 13 functions, 0 classes
        test_database_manager_api.py              # 82 lines, 1 functions, 0 classes
        test_database_manager_backup.py           # 74 lines, 4 functions, 0 classes
        test_database_manager_cache.py            # 83 lines, 4 functions, 0 classes
        test_database_manager_failover.py         # 84 lines, 5 functions, 0 classes
        test_database_manager_integration.py      # 98 lines, 7 functions, 0 classes
        test_database_manager_logging.py          # 77 lines, 4 functions, 0 classes
        test_database_manager_metrics.py          # 0 lines, 0 functions, 0 classes
        test_database_manager_migration.py        # 91 lines, 4 functions, 0 classes
        test_database_manager_performance.py      # 103 lines, 9 functions, 0 classes
        test_database_manager_realtime.py         # 82 lines, 1 functions, 0 classes
        test_database_manager_recovery.py         # 84 lines, 4 functions, 0 classes
        test_database_manager_stress.py           # 104 lines, 6 functions, 0 classes
        test_database_manager_transactions.py     # 90 lines, 5 functions, 0 classes
        test_database_manager_validation.py       # 96 lines, 5 functions, 0 classes
        test_gen_structure.py                     # 184 lines, 12 functions, 0 classes
        test_schema_runner.py                     # 930 lines, 38 functions, 3 classes
    utils/
        gen_structure.py                          # 186 lines, 7 functions, 1 classes
```