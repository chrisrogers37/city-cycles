import os
import zipfile
from io import BytesIO

class Node:
    """
    Base class for any node in the file tree (folder, file, zip, etc.)
    """
    def __init__(self, name, parent=None):
        self.name = name
        self.parent = parent

class Folder(Node):
    """
    Represents a directory/folder, which can contain children (folders/files/zips)
    """
    def __init__(self, name, parent=None):
        super().__init__(name, parent)
        self.children = []

    def add_child(self, child):
        self.children.append(child)

class File(Node):
    """
    Represents a regular file (e.g., CSV). Content is bytes or BytesIO.
    """
    def __init__(self, name, content, parent=None):
        super().__init__(name, parent)
        self.content = content  # bytes or BytesIO

class ZipFile(File):
    """
    Represents a ZIP file. Can extract itself into a Folder tree.
    """
    def extract(self):
        """
        Extracts the ZIP file and returns a Folder representing its contents.
        Recursively processes nested ZIPs and folders.
        """
        folder = Folder(self.name, self.parent)
        with zipfile.ZipFile(BytesIO(self.content)) as zf:
            # Debug: List all files in the ZIP
            print(f"DEBUG: Files in ZIP '{self.name}':")
            for info in zf.infolist():
                print(f"  - {info.filename} (dir: {info.is_dir()})")
            
            # Process files and folders
            for info in zf.infolist():
                data = zf.read(info.filename)
                
                # Handle nested ZIPs
                if info.filename.lower().endswith('.zip'):
                    print(f"DEBUG: Found nested ZIP: {info.filename}")
                    child = ZipFile(info.filename, data, folder)
                    folder.add_child(child.extract())
                
                # Handle CSV files (case-insensitive), excluding Mac OS hidden files
                elif info.filename.lower().endswith('.csv') and not info.filename.startswith('._'):
                    print(f"DEBUG: Found CSV file: {info.filename}")
                    child = File(info.filename, data, folder)
                    folder.add_child(child)
                
                # Handle directories by creating folder structure
                elif info.is_dir():
                    print(f"DEBUG: Found directory: {info.filename}")
                    # Create folder structure if needed
                    path_parts = info.filename.rstrip('/').split('/')
                    current_folder = folder
                    
                    for part in path_parts:
                        if part:  # Skip empty parts
                            # Find or create the subfolder
                            existing_child = None
                            for child in current_folder.children:
                                if isinstance(child, Folder) and child.name == part:
                                    existing_child = child
                                    break
                            
                            if existing_child:
                                current_folder = existing_child
                            else:
                                new_folder = Folder(part, current_folder)
                                current_folder.add_child(new_folder)
                                current_folder = new_folder
                
                # Handle CSV files that are Mac OS hidden files (skip them)
                elif info.filename.lower().endswith('.csv') and info.filename.startswith('._'):
                    print(f"DEBUG: Skipping Mac OS hidden CSV file: {info.filename}")
                
                else:
                    print(f"DEBUG: Found other file: {info.filename}")
                    # Optionally handle other file types
                    pass
        
        print(f"DEBUG: Extracted folder '{self.name}' contains {len(folder.children)} children")
        return folder

def walk_folder(folder):
    """
    Recursively yield all File objects in the folder tree.
    """
    for child in folder.children:
        if isinstance(child, Folder):
            yield from walk_folder(child)
        elif isinstance(child, File):
            yield child 