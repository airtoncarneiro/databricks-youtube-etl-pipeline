# databricks-youtube-etl-pipeline (em desenvolvimento)

Pipeline assíncrono em Python para ingestão de metadados do YouTube Data API v3, com saída em NDJSON compactado (`.ndjson.gz`) particionado por data de ingestão.

Estado atual: funcional para ingestão por `channel_id` via CLI, com suporte a múltiplos canais, limitação de taxa (RPS), retry com backoff e rotação de arquivos por tamanho.

## O que o projeto faz

Para cada canal informado:

1. Busca metadados do canal (`channels.list`)
2. Lista vídeos do canal (`search.list`)
3. Busca detalhes dos vídeos em lotes (`videos.list`)
4. Escreve os dados em arquivos NDJSON gzip em duas camadas:
   - `youtube/channels`
   - `youtube/videos`

## Estrutura do projeto

```text
.
├── databricks.yml
├── pyproject.toml
├── requirements.txt
└── yt_ingest/
    ├── cli.py
    ├── config.py
    ├── http_client.py
    ├── io_ndjson.py
    ├── pipeline.py
    ├── rate_limiter.py
    ├── utils.py
    └── youtube_api.py
```

## Pré-requisitos

- Python `>=3.10` (definido em `pyproject.toml`)
- Chave da YouTube Data API v3

## Instalação

### Ambiente mínimo (recomendado para CLI)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

### Ambiente completo (stack de notebooks/Databricks local)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuração

Defina a chave da API:

```bash
export YOUTUBE_API_KEY="sua_chave_aqui"
```

Também é possível passar a chave explicitamente via `--api-key`.

## Uso da CLI

Após instalar com `pip install -e .`, o comando `yt-ingest` fica disponível.

Exemplo com um canal:

```bash
yt-ingest \
  --channel-id UCELku_rf-FHbWhLIwsKLGGA \
  --output-root ./raw
```

Exemplo com múltiplos canais:

```bash
yt-ingest \
  --channel-id "UCELku_rf-FHbWhLIwsKLGGA,UCVHFbqXqoYvEWM1Ddxl0QDg" \
  --max-videos 100 \
  --rps 8 \
  --part-size-mb 32
```

### Parâmetros

- `--api-key`: chave da YouTube API (fallback para `YOUTUBE_API_KEY`)
- `--channel-id`: um ou vários IDs (separados por vírgula ou espaço)
- `--output-root`: diretório raiz de saída (default: `./raw`)
- `--ingestion-date`: data da partição no formato `AAAA-MM-DD` (default: data atual)
- `--rps`: limite de requisições por segundo (default: `8`)
- `--part-size-mb`: rotação aproximada de arquivo NDJSON antes da compressão (default: `32`)
- `--max-videos`: máximo de vídeos por canal via `search.list` (default: `50`)

## Layout de saída

```text
raw/
└── youtube/
    ├── channels/
    │   └── ingestion_date=2026-02-20/
    │       ├── part-00000.ndjson.gz
    │       └── ...
    └── videos/
        └── ingestion_date=2026-02-20/
            ├── part-00000.ndjson.gz
            └── ...
```

## Formato dos registros

Registros em `channels`:

- `_type: "youtube_channel"` para payloads retornados por `channels.list`
- `_type: "info"` com `message: "no_videos_found_via_search_list"` quando não há vídeos

Registros em `videos`:

- `_type: "youtube_video"` para payloads retornados por `videos.list`
- `_type: "not_found"` para vídeos retornados no `search.list` que não vieram no `videos.list`

Campos de auditoria comuns:

- `ingestion_ts` (UTC)
- `source` (`youtube_api_v3`)
- `entity` (`channels` ou `videos`)
- `channelId`
- `videoId` (quando aplicável)
- `payload` (objeto JSON bruto da API, quando aplicável)

## Comportamento de rede e resiliência

- Cliente HTTP assíncrono com `aiohttp`
- Retry automático para `429`, `500`, `502`, `503`, `504`
- Backoff exponencial configurado em `MAX_RETRIES=5`
- Limitação de taxa por janela de ~1s via `RateLimiter`
- Escrita atômica dos arquivos (`.tmp` + `os.replace`)

## Databricks

O projeto inclui um `databricks.yml` com bundle chamado `databricks` e target `dev` já apontando para um workspace.

Atualmente o bundle define o ambiente, mas não possui recursos (jobs/pipelines) declarados no arquivo.

## Limitações atuais

- Descoberta de canal por `for_username` e `handle_or_query` está reservada no código, mas não implementada na CLI.
- Não há suíte de testes automatizados no repositório.
- `requirements.txt` inclui dependências além do necessário para execução mínima da CLI.

## Referência rápida de canais para teste

- Portal do José: `UCELku_rf-FHbWhLIwsKLGGA`
- Amazon: `UCVHFbqXqoYvEWM1Ddxl0QDg`
