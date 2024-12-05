# utils/gen_structure.py

import os
import ast
from datetime import datetime
from typing import Dict, List, Tuple
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

class ProjectStructureGenerator:
    def __init__(self, root_dir: str = "."):
        self.root_dir = Path(root_dir)
        self.logger = logging.getLogger(__name__)
        self.reset_stats()
        
        self.excluded_dirs = {'.git', '.pytest_cache', 'venv', '__pycache__'}
        self.binary_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.pdf', '.exe'}
        
    def reset_stats(self):
        """Reset all statistics counters"""
        self.stats = {
            "total_files": 0,
            "total_lines": 0,
            "python_files": 0,
            "python_lines": 0,
            "functions": 0,
            "classes": 0,
            "sql_files": 0,
            "md_files": 0,
            "other_files": 0,
            "languages": {}
        }

    @lru_cache(maxsize=1000)
    def analyze_python_file(self, file_path: str) -> Tuple[int, int, int, str, str]:
        """Analyze Python file and return (lines, functions, classes, title, description)"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                lines = len(content.splitlines())
                
            tree = ast.parse(content)
            
            functions = sum(1 for node in ast.walk(tree) 
                          if isinstance(node, ast.FunctionDef))
            classes = sum(1 for node in ast.walk(tree) 
                         if isinstance(node, ast.ClassDef))
            
            # Extract docstring if present
            module_doc = ast.get_docstring(tree)
            if module_doc:
                title, *desc_lines = module_doc.split('\n')
                description = ' '.join(line.strip() for line in desc_lines if line.strip())
            else:
                title = os.path.basename(file_path)
                description = "No description available"
                
            return lines, functions, classes, title, description
            
        except Exception as e:
            self.logger.error(f"Error analyzing {file_path}: {str(e)}")
            return 0, 0, 0, "", ""

    def process_file(self, file_path: str) -> str:
        """Process a single file and update statistics"""
        if not os.path.exists(file_path):
            return ""
            
        self.stats['total_files'] += 1
        
        if file_path.endswith('.py'):
            lines, funcs, classes, _, _ = self.analyze_python_file(file_path)
            self.stats['python_files'] += 1
            self.stats['python_lines'] += lines
            self.stats['functions'] += funcs
            self.stats['classes'] += classes
            self.stats['total_lines'] += lines
            self.stats['languages']['Python'] = self.stats['languages'].get('Python', 0) + 1
            return f"# {lines} lines, {funcs} functions, {classes} classes"
            
        elif file_path.endswith('.sql'):
            self.stats['sql_files'] += 1
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = len(f.readlines())
                self.stats['total_lines'] += lines
                self.stats['languages']['SQL'] = self.stats['languages'].get('SQL', 0) + 1
                return f"# {lines} lines"
                
        elif file_path.endswith('.md'):
            self.stats['md_files'] += 1
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = len(f.readlines())
                self.stats['total_lines'] += lines
                self.stats['languages']['Markdown'] = self.stats['languages'].get('Markdown', 0) + 1
                return f"# {lines} lines"
                
        else:
            self.stats['other_files'] += 1
            return ""

    def generate_tree(self) -> str:
        """Generate directory tree and collect statistics"""
        self.reset_stats()
        output = []
        
        with ThreadPoolExecutor() as executor:
            for root, dirs, files in os.walk(self.root_dir):
                dirs[:] = [d for d in dirs if d not in self.excluded_dirs]
                
                level = Path(root).relative_to(self.root_dir).parts
                indent = '    ' * len(level)
                
                if root != self.root_dir:
                    output.append(f"{indent}{os.path.basename(root)}/")
                
                sub_indent = '    ' * (len(level) + 1)
                
                # Process files in parallel
                file_paths = [os.path.join(root, f) for f in sorted(files)
                            if not f.startswith('.') and not f.startswith('__pycache__')]
                
                future_to_file = {executor.submit(self.process_file, fp): fp 
                                for fp in file_paths}
                
                for future in future_to_file:
                    file_path = future_to_file[future]
                    try:
                        stats = future.result()
                        if stats:  # Only add files with statistics
                            fname = os.path.basename(file_path)
                            padding = ' ' * (50 - len(fname) - len(sub_indent))
                            output.append(f"{sub_indent}{fname}{padding}{stats}")
                    except Exception as e:
                        self.logger.error(f"Error processing {file_path}: {str(e)}")
        
        return '\n'.join(output)

    def generate_statistics(self) -> str:
        """Generate statistics section"""
        sorted_languages = sorted(
            self.stats['languages'].items(),
            key=lambda x: (-x[1], x[0])
        )
        
        return '\n'.join([
            "## Project Statistics",
            f"- Total Files: {self.stats['total_files']}",
            f"- Total Lines: {self.stats['total_lines']}",
            f"- Python Lines: {self.stats['python_lines']}",
            f"- Total Functions: {self.stats['functions']}",
            f"- Total Classes: {self.stats['classes']}",
            f"- Last Updated: {datetime.now()}",
            "",
            "### Languages",
            *(f"- {lang}: {count} files" for lang, count in sorted_languages)
        ])

    def generate_markdown(self) -> str:
        """Generate complete markdown documentation"""
        tree = self.generate_tree()  # Generate tree first to collect stats
        
        return '\n'.join([
            "# Project Structure",
            "",
            f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            self.generate_statistics(),
            "",
            "## Directory Structure",
            "```",
            tree,
            "```"
        ])

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                       format='%(asctime)s - %(levelname)s - %(message)s')
    
    docs_dir = Path(__file__).parent.parent / 'docs'
    docs_dir.mkdir(exist_ok=True)
    
    generator = ProjectStructureGenerator()
    with open(docs_dir / "PROJECT_STRUCTURE.md", "w", encoding='utf-8') as f:
        f.write(generator.generate_markdown())