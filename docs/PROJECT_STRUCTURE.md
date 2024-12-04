# Project Structure

Generated on: 2024-12-04 13:01:16

## Project Statistics
- Total Files: 30
- Total Lines: 2050
- Python Lines: 1877
- Total Functions: 76
- Total Classes: 32
- Last Updated: 2024-12-04 13:00:23.050190

### Languages
- Python: 26 files
- SQL: 2 files
- Markdown: 1 files
- Other: 1 files

## Directory Structure
```
    __init__.py
    quantum_data_manager.code-workspace
    db/
        backups/
            backup_20241202_233709.sql
        migrations/
            schema.sql                     # 113 lines, 0 functions, 0 classes
    docs/
        PROJECT_STRUCTURE.md           # 60 lines, 0 functions, 0 classes
    src/
        data_fetcher.py                # 320 lines, 14 functions, 1 classes
        data_manager.py                # 266 lines, 4 functions, 3 classes
        database_client.py             # 53 lines, 5 functions, 1 classes
        database_config.py             # 44 lines, 3 functions, 2 classes
        database_manager.py            # 22 lines, 6 functions, 1 classes
        mock_api.py                    # 41 lines, 3 functions, 1 classes
        models.py                      # 185 lines, 13 functions, 8 classes
        polygon_client.py              # 65 lines, 2 functions, 1 classes
        api/
            __init__.py
            endpoints.py                   # 87 lines, 0 functions, 3 classes
        cache/
            __init__.py
            memory_cache.py                # 96 lines, 6 functions, 3 classes
        fetch_modules/
            __init__.py
            base/
                __init__.py
                data_source_base.py            # 23 lines, 0 functions, 1 classes
            mock/
                __init__.py
                mock_data_source.py            # 116 lines, 8 functions, 3 classes
            polygon/
                __init__.py                    # 3 lines, 0 functions, 0 classes
                fetch_polygon_data.py          # 111 lines, 2 functions, 0 classes
                polygon_client.py              # 18 lines, 2 functions, 1 classes
                polygon_data_source.py         # 74 lines, 1 functions, 1 classes
                polygon_fetch_data.py          # 29 lines, 0 functions, 0 classes
        managers/
            __init__.py
            market_data.py                 # 161 lines, 1 functions, 1 classes
    utils/
        gen_structure.py               # 163 lines, 6 functions, 1 classes
```