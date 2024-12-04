# Project Structure

Generated on: 2024-12-04 15:35:04

## Project Statistics
- Total Files: 0
- Total Lines: 0
- Python Lines: 0
- Total Functions: 0
- Total Classes: 0
- Last Updated: 2024-12-04 15:35:04.365957

### Languages
- Python: 0 files
- Other: 0 files
- SQL: 0 files
- Markdown: 0 files


## Directory Structure
```

    __init__.py                                                             
    pytest.ini                                                              
    quantum_data_manager.code-workspace                                          
    requirements.txt                                                        
    setup.py                       # 27 lines, 0 functions, 0 classes       
    db/
        backups/
            backup_20241202_233709.sql                                              
        migrations/
            schema.sql                                                              
    docs/
        PROJECT_STRUCTURE.md                                                    
        README.md                                                               
    src/
        data_fetcher.py                # 378 lines, 14 functions, 1 classes     
        data_manager.py                # 326 lines, 4 functions, 3 classes      
        database_client.py             # 60 lines, 5 functions, 1 classes       
        database_config.py             # 25 lines, 1 functions, 2 classes       
        database_manager.py            # 38 lines, 6 functions, 1 classes       
        exceptions.py                  # 8 lines, 0 functions, 2 classes        
        models.py                      # 151 lines, 5 functions, 8 classes      
        api/
            __init__.py                                                             
            endpoints.py                   # 113 lines, 1 functions, 3 classes      
        cache/
            __init__.py                                                             
            memory_cache.py                # 137 lines, 9 functions, 5 classes      
        fetch_modules/
            __init__.py                                                             
            base/
                __init__.py                                                             
                base_data_source_.py           # 38 lines, 0 functions, 1 classes       
            mock/
                __init__.py                                                             
                mock_api.py                    # 57 lines, 3 functions, 1 classes       
                mock_data_source.py            # 139 lines, 8 functions, 3 classes      
            polygon/
                __init__.py                    # 4 lines, 0 functions, 0 classes        
                polygon_client.py              # 33 lines, 2 functions, 1 classes       
                polygon_data_source.py         # 88 lines, 1 functions, 1 classes       
                polygon_fetch_data.py          # 53 lines, 1 functions, 1 classes       
        managers/
            __init__.py                                                             
            market_data.py                 # 195 lines, 1 functions, 1 classes      
    tests/
        test_data_manager.py           # 39 lines, 1 functions, 0 classes       
        test_gen_structure.py          # 184 lines, 12 functions, 0 classes     
    utils/
        gen_structure.py               # 162 lines, 7 functions, 1 classes      
```