# utils/generate_project_structure.py

import os
import ast
from datetime import datetime
from typing import Dict, List
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import logging

EXCLUDE_DIRS = {'.git', '.pytest_cache', 'venv', '__pycache__'}
DOCS_DIR = Path(__file__).parent.parent / 'docs'
BINARY_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.pdf', '.exe', '.bin', '.pyc'}
TEXT_EXTENSIONS = {'.py', '.js', '.vue', '.html', '.css', '.sql', '.yml', '.yaml', '.sh', '.md', '.txt'}
BINARY_FILES = {'requirements.txt', 'poetry.lock'}


class ProjectAnalyzer:
    def __init__(self, root_dir: str):
        self.root_dir = Path(root_dir)
        self.structure = []
        self.metadata = {
            'total_files': 0,
            'total_lines': 0,
            'total_python_lines': 0,
            'total_functions': 0,
            'total_classes': 0,
            'languages': {},
            'last_updated': None,
            'directories': {}
        }
        self.language_mappings = {
            '.py': 'Python', '.js': 'JavaScript', '.vue': 'Vue',
            '.html': 'HTML', '.css': 'CSS', '.sql': 'SQL',
            '.yml': 'YAML', '.yaml': 'YAML', '.sh': 'Shell',
            '.md': 'Markdown'
        }
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    @lru_cache(maxsize=1000)
    def analyze_file(self, filepath: str) -> Dict:
        """Analyze file with caching"""
        stats = {'lines': 0, 'functions': 0, 'classes': 0, 'docstrings': 0, 'language': 'Unknown'}
        path = Path(filepath)

        try:
            ext = path.suffix.lower()
            stats['language'] = self.language_mappings.get(ext, 'Other')

            if not path.exists() or not path.is_file():
                return stats

            # Skip binary files and specific files
            if ext in BINARY_EXTENSIONS or path.name in BINARY_FILES:
                return stats

            # Only analyze text files
            if ext in TEXT_EXTENSIONS:
                try:
                    content = path.read_text(encoding='utf-8', errors='ignore')
                    stats['lines'] = sum(1 for line in content.splitlines() if line.strip())

                    if ext == '.py':
                        tree = ast.parse(content)
                        stats['functions'] = sum(1 for n in ast.walk(tree) if isinstance(n, ast.FunctionDef))
                        stats['classes'] = sum(1 for n in ast.walk(tree) if isinstance(n, ast.ClassDef))
                        stats['docstrings'] = sum(1 for n in ast.walk(tree)
                                                  if isinstance(n, ast.Expr) and isinstance(n.value, ast.Str))
                except UnicodeDecodeError:
                    self.logger.debug(f"Skipping binary file: {filepath}")

        except Exception as e:
            self.logger.error(f"Error analyzing {filepath}: {str(e)}")

        return stats

    def analyze_directory(self, directory: Path) -> Dict:
        """Analyze a single directory"""
        stats = {'files': 0, 'lines': 0}

        try:
            for item in directory.iterdir():
                if item.is_file():
                    file_stats = self.analyze_file(str(item))
                    stats['files'] += 1
                    stats['lines'] += file_stats['lines']
        except Exception as e:
            self.logger.error(f"Error analyzing directory {directory}: {str(e)}")

        return stats

    def generate_structure(self) -> List[str]:
        """Generate structure with parallel processing"""
        self.logger.info("Starting project structure analysis...")

        with ThreadPoolExecutor() as executor:
            for root, dirs, files in os.walk(self.root_dir):
                root_path = Path(root)
                dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]

                self.logger.info(f"Analyzing directory: {root_path}")

                # Parallel directory analysis
                future_to_dir = {executor.submit(self.analyze_directory, root_path): root_path}
                for future in future_to_dir:
                    dir_path = future_to_dir[future]
                    try:
                        stats = future.result()
                        self.metadata['directories'][str(dir_path)] = stats
                    except Exception as e:
                        self.logger.error(f"Error processing directory {dir_path}: {str(e)}")

                # Format output
                level = len(root_path.relative_to(self.root_dir).parts)
                indent = ' ' * 4 * level

                if root_path != self.root_dir:
                    self.structure.append(f"{indent}{root_path.name}/")

                sub_indent = ' ' * 4 * (level + 1)
                for f in sorted(files):
                    filepath = root_path / f
                    stats = self.analyze_file(str(filepath))

                    self._update_metadata(stats, filepath)

                    if any(v > 0 for k, v in stats.items() if k != 'language'):
                        self.structure.append(
                            f"{sub_indent}{f:<30} # {stats['lines']} lines, "
                            f"{stats['functions']} functions, {stats['classes']} classes"
                        )
                    else:
                        self.structure.append(f"{sub_indent}{f}")

        self.logger.info("Analysis complete.")
        return self.structure

    def _update_metadata(self, stats: Dict, filepath: Path) -> None:
        """Update metadata atomically"""
        self.metadata['total_files'] += 1
        self.metadata['total_lines'] += stats['lines']

        if stats['language'] == 'Python':
            self.metadata['total_python_lines'] += stats['lines']
            self.metadata['total_functions'] += stats['functions']
            self.metadata['total_classes'] += stats['classes']

        if stats['language'] != 'Unknown':
            self.metadata['languages'][stats['language']] = \
                self.metadata['languages'].get(stats['language'], 0) + 1

        mtime = datetime.fromtimestamp(filepath.stat().st_mtime)
        if not self.metadata['last_updated'] or mtime > self.metadata['last_updated']:
            self.metadata['last_updated'] = mtime

    def save_documentation(self) -> None:
        """Save project structure to markdown file"""
        DOCS_DIR.mkdir(parents=True, exist_ok=True)  # Ensure the docs directory exists
        output_file = DOCS_DIR / 'PROJECT_STRUCTURE.md'
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        sorted_languages = sorted(
            self.metadata['languages'].items(),
            key=lambda x: (-x[1], x[0])  # Sort by count desc, then name asc
        )

        with open(output_file, 'w', encoding='utf-8', newline='\n') as f:
            f.write("# Project Structure\n\n")
            f.write(f"Generated on: {timestamp}\n\n")

            # Write statistics
            f.write("## Project Statistics\n")
            f.write(f"- Total Files: {self.metadata['total_files']}\n")
            f.write(f"- Total Lines: {self.metadata['total_lines']}\n")
            f.write(f"- Python Lines: {self.metadata['total_python_lines']}\n")
            f.write(f"- Total Functions: {self.metadata['total_functions']}\n")
            f.write(f"- Total Classes: {self.metadata['total_classes']}\n")
            f.write(f"- Last Updated: {self.metadata['last_updated']}\n\n")

            # Write sorted language statistics
            f.write("### Languages\n")
            for lang, count in sorted_languages:
                f.write(f"- {lang}: {count} files\n")

            # Write directory structure
            f.write("\n## Directory Structure\n```\n")
            f.write('\n'.join(self.structure))
            f.write("\n```")

        self.logger.info(f"Documentation saved to {output_file}")


if __name__ == '__main__':
    analyzer = ProjectAnalyzer('.')
    analyzer.generate_structure()
    analyzer.save_documentation()