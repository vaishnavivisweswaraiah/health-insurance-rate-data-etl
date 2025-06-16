from pathlib import Path

def find_project_root(path=Path.cwd(), marker='requirements.txt'):
    for parent in [path] + list(path.parents):
        if (parent / marker).exists():
            return parent
    return path
