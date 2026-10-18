# src/meret/vfs/client.py

def list_dir(self, vpath: str) -> list[DirEntry]:
    """List directory contents."""
    logger.debug(f"list_dir({vpath})")
    
    # Получаем file_ids из виртуальной структуры
    file_ids = self._get_files_in_dir(vpath)
    
    entries = []
    for file_id in file_ids:
        file_info = self.repo.get_file(file_id)
        if not file_info:
            continue
        
        # Формируем правильный vpath для каждого файла
        entry_vpath = f"{vpath}/{file_info['name']}".replace("//", "/")
        
        entry = DirEntry(
            name=file_info["name"],
            vpath=entry_vpath,  # ВАЖНО!
            is_dir=file_info.get("is_dir", False),
            size=file_info.get("size", 0),
            mtime=file_info.get("mtime", 0.0),
        )
        entries.append(entry)
    
    logger.debug(f"Returning {len(entries)} entries with vpaths")
    return entries