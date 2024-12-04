# utils/gen_structure.py

import os
from datetime import datetime
import logging
from typing import Dict, List, Tuple
import ast

class ProjectStructureGenerator:
    """Generates project structure documentation"""
    
    def __init__(self, root_dir: str = "."):
        self.root_dir = root_dir
        self.logger = logging.getLogger(__name__)
        self.stats = {
            "total_files": 0,
            "total_lines": 0,
            "python_files": 0,
            "python_lines": 0,
            "functions": 0,
            "classes": 0
        }

    def analyze_python_file(self, file_path: str) -> Tuple[int, int, int, str, str]:
        """Analyze a Python file for statistics and documentation"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content)
            
            # Get file docstring if exists
            title = ""
            description = ""
            for node in ast.walk(tree):
                if isinstance(node, ast.Module) and ast.get_docstring(node):
                    doc = ast.get_docstring(node)
                    parts = doc.split('\n', 1)
                    title = parts[0].strip()
                    description = parts[1].strip() if len(parts) > 1 else ""
                    break
            
            functions = len([node for node in ast.walk(tree) 
                           if isinstance(node, ast.FunctionDef)])
            classes = len([node for node in ast.walk(tree) 
                         if isinstance(node, ast.ClassDef)])
            lines = len(content.splitlines())
            
            return lines, functions, classes, title, description
            
        except Exception as e:
            self.logger.error(f"Error analyzing {file_path}: {e}")
            return 0, 0, 0, "", ""

    def generate_tree(self) -> str:
        """Generate directory tree structure"""
        output = []
        excluded_dirs = {'.git', '__pycache__', 'venv', '.pytest_cache', '.venv'}
        
        for root, dirs, files in os.walk(self.root_dir):
            # Skip excluded directories
            dirs[:] = [d for d in dirs if not d.startswith('.') 
                      and d not in excluded_dirs]
            
            # Calculate indent level
            level = root.replace(self.root_dir, '').count(os.sep)
            indent = '    ' * level
            
            # Add directory name
            path = os.path.basename(root)
            if level == 0:
                output.append('')
            else:
                output.append(f'{indent}{path}/')
            
            # Add files with statistics
            subindent = '    ' * (level + 1)
            for f in sorted(files):
                if f.startswith('.'):
                    continue
                    
                file_path = os.path.join(root, f)
                stats = ""
                desc = ""
                
                if f.endswith('.py'):
                    lines, funcs, classes, title, description = self.analyze_python_file(file_path)
                    if lines > 0:
                        stats = f"# {lines} lines, {funcs} functions, {classes} classes"
                        if title:
                            desc = f" - {title}"
                        
                output.append(f'{subindent}{f:<30} {stats:<40} {desc}')
                
                # Update statistics
                self.stats['total_files'] += 1
                self.stats['total_lines'] += lines
                if f.endswith('.py'):
                    self.stats['python_files'] += 1
                    self.stats['python_lines'] += lines
                    self.stats['functions'] += funcs
                    self.stats['classes'] += classes
                    
        return '\n'.join(output)

    def generate_statistics(self) -> str:
        """Generate project statistics section"""
        stats = []
        stats.append("## Project Statistics")
        stats.append(f"- Total Files: {self.stats['total_files']}")
        stats.append(f"- Total Lines: {self.stats['total_lines']}")
        stats.append(f"- Python Lines: {self.stats['python_lines']}")
        stats.append(f"- Total Functions: {self.stats['functions']}")
        stats.append(f"- Total Classes: {self.stats['classes']}")
        stats.append(f"- Last Updated: {datetime.now()}")
        return '\n'.join(stats)

    def generate_markdown(self) -> str:
        """Generate project structure documentation in markdown format"""
        output = ["# Project Structure\n"]
        
        # Add generation timestamp
        output.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # Add project statistics and languages sections
        output.append(self.generate_statistics())
        
        output.append("\n### Languages")
        output.append(f"- Python: {self.stats['python_files']} files")
        output.append(f"- Other: {self.stats['total_files'] - self.stats['python_files']} files")
        output.append(f"- SQL: {len([f for f in os.listdir() if f.endswith('.sql')])} files")
        output.append(f"- Markdown: {len([f for f in os.listdir() if f.endswith('.md')])} files\n")
        
        # Add directory structure
        output.append("\n## Directory Structure")
        output.append("```")
        output.append(self.generate_tree())
        output.append("```")
        
        return '\n'.join(output)

def main():
    """Generate and save project structure documentation"""
    generator = ProjectStructureGenerator()
    markdown = generator.generate_markdown()
    
    output_path = os.path.join("docs", "PROJECT_STRUCTURE.md")
    os.makedirs("docs", exist_ok=True)
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(markdown)
    
    print(f"Project structure documentation generated: {output_path}")

def verify_documentation():
    with open("docs/PROJECT_STRUCTURE.md", "r") as f:
        content = f.read()
    assert "Project Structure" in content
    assert "Directory Structure" in content

if __name__ == "__main__":
    main()