from .utils import compose_command as _compose_command

__all__ = ["compose_command_for_file"]

def compose_command_for_file(file:str, default_command_map={}) -> str:
    return _compose_command(file, default_command_map)