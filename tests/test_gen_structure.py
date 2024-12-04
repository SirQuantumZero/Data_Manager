# tests/test_gen_structure.py

import pytest
import os
from datetime import datetime
from utils.gen_structure import ProjectStructureGenerator

@pytest.fixture
def temp_project_dir(tmp_path):
    """Create a temporary project structure for testing"""
    # Create test files
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "main.py").write_text('''"""Main module
    
    This is the main entry point
    """
    def main():
        pass
    ''')
    
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_main.py").write_text('''
    def test_something():
        assert True
    ''')
    
    return tmp_path

def test_analyze_python_file(temp_project_dir):
    """Test Python file analysis"""
    generator = ProjectStructureGenerator(str(temp_project_dir))
    main_py = str(temp_project_dir / "src" / "main.py")
    
    lines, funcs, classes, title, desc = generator.analyze_python_file(main_py)
    
    assert lines > 0
    assert funcs == 1
    assert classes == 0
    assert title == "Main module"
    assert "main entry point" in desc

def test_generate_tree(temp_project_dir):
    """Test directory tree generation"""
    generator = ProjectStructureGenerator(str(temp_project_dir))
    tree = generator.generate_tree()
    
    assert "src/" in tree
    assert "tests/" in tree
    assert "main.py" in tree
    assert "test_main.py" in tree

def test_generate_markdown(temp_project_dir):
    """Test markdown documentation generation"""
    generator = ProjectStructureGenerator(str(temp_project_dir))
    markdown = generator.generate_markdown()
    
    assert "# Project Structure" in markdown
    assert "## Project Statistics" in markdown
    assert "### Languages" in markdown
    assert "## Directory Structure" in markdown

def test_generate_statistics(temp_project_dir):
    """Test statistics generation"""
    generator = ProjectStructureGenerator(str(temp_project_dir))
    
    # Process files to gather stats
    generator.generate_tree()
    stats = generator.generate_statistics()
    
    assert "Project Statistics" in stats
    assert "Total Files:" in stats
    assert "Python Files:" in stats
    assert generator.stats["python_files"] == 2  # main.py and test_main.py
    assert generator.stats["total_files"] >= 2
    assert generator.stats["functions"] >= 2  # main() and test_something()
    assert "Total Files:" in stats
    assert "Python Lines:" in stats
    assert generator.stats["python_files"] == 2
    assert generator.stats["functions"] >= 1
    assert generator.stats["classes"] == 0

def test_empty_directory(tmp_path):
    """Test handling of empty directory"""
    generator = ProjectStructureGenerator(str(tmp_path))
    tree = generator.generate_tree()
    
    assert tree != ""
    assert generator.stats["total_files"] == 0
    assert generator.stats["python_files"] == 0

def test_invalid_python_file(temp_project_dir):
    """Test handling of invalid Python file"""
    bad_py = temp_project_dir / "src" / "bad.py"
    bad_py.write_text("this is not valid python!")
    
    generator = ProjectStructureGenerator(str(temp_project_dir))
    lines, funcs, classes, title, desc = generator.analyze_python_file(str(bad_py))
    
    assert lines == 0
    assert funcs == 0
    assert classes == 0
    assert title == ""
    assert desc == ""

def test_nested_directories(tmp_path):
    """Test handling of nested directory structures"""
    # Create nested structure
    (tmp_path / "src" / "core" / "data").mkdir(parents=True)
    (tmp_path / "src" / "core" / "data" / "manager.py").write_text('''
        """Data Manager
        Handles data operations
        """
        class DataManager:
            def __init__(self):
                pass
    ''')

    generator = ProjectStructureGenerator(str(tmp_path))
    tree = generator.generate_tree()
    
    assert "src/" in tree
    assert "core/" in tree
    assert "data/" in tree
    assert "manager.py" in tree
    assert generator.stats["classes"] == 1

def test_file_exclusions(tmp_path):
    """Test exclusion of hidden files and __pycache__"""
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / ".hidden.py").write_text("# hidden")
    (tmp_path / "src" / "__pycache__").mkdir()
    (tmp_path / "src" / "__pycache__" / "cache.pyc").write_text("")
    
    generator = ProjectStructureGenerator(str(tmp_path))
    tree = generator.generate_tree()
    
    assert ".hidden.py" not in tree
    assert "__pycache__" not in tree
    assert "cache.pyc" not in tree

def test_multiple_file_types(tmp_path):
    """Test handling of different file types"""
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "schema.sql").write_text("CREATE TABLE test;")
    (tmp_path / "src" / "README.md").write_text("# README")
    
    generator = ProjectStructureGenerator(str(tmp_path))
    markdown = generator.generate_markdown()
    
    assert "SQL: 1 files" in markdown
    assert "Markdown: 1 files" in markdown

def test_markdown_output_format(temp_project_dir):
    """Test markdown formatting is correct"""
    generator = ProjectStructureGenerator(str(temp_project_dir))
    markdown = generator.generate_markdown()
    
    # Check sections are properly formatted
    assert markdown.startswith("# Project Structure\n")
    assert "\n## Project Statistics" in markdown
    assert "\n### Languages" in markdown
    assert "\n```" in markdown
    assert "```\n" in markdown

def test_error_handling(temp_project_dir):
    """Test error handling for invalid files/paths"""
    generator = ProjectStructureGenerator(str(temp_project_dir))
    
    # Test non-existent file
    lines, funcs, classes, title, desc = generator.analyze_python_file("nonexistent.py")
    assert lines == 0
    assert funcs == 0
    assert classes == 0
    
    # Test invalid Python syntax
    bad_file = temp_project_dir / "bad.py"
    bad_file.write_text("def invalid syntax{")
    lines, funcs, classes, title, desc = generator.analyze_python_file(str(bad_file))
    assert lines == 0
    assert funcs == 0
    assert classes == 0

if __name__ == "__main__":
    pytest.main([__file__])