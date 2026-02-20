# yt-ingest (em desenvolvimento)

Ingesta metadados de canais do YouTube (search.list + videos.list) e escreve NDJSON `.gz` particionado por `ingestion_date`.

## Instalação

```bash
pip install -e .
```

Canais para testes:
TEST_CHANNEL_ID = ["UCELku_rf-FHbWhLIwsKLGGA",      # Portal do José
                   "UCVHFbqXqoYvEWM1Ddxl0QDg"]      # Amazon