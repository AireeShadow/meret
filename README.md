# meret 🐍

Тегированная медиатека: теги вместо папок, виртуальная иерархия, GUI.

## Установка
pip install -e ".[dev]"

## Быстрый старт
meret init ~/Media
meret scan ~/Media --db ~/Media
meret tag ~/Media/photo.jpg отпуск пляж
meret search --tag отпуск
meret tree