# DB Analysis Agent 🤖

Interaktivní AI agent pro analýzu investičních dat z MySQL databáze.

## Instalace

1. Přidejte `OPENROUTER_API_KEY` do `.env` souboru v kořenovém adresáři:
   ```
   OPENROUTER_API_KEY=sk-or-v1-xxxxx
   ```

2. Nainstalujte závislosti:
   ```bash
   pip install openai pyyaml
   ```

## Spuštění

```bash
cd c:\workspace\py_invest
python -m stocks.agent.db_agent
```

## Příkazy

| Příkaz | Popis |
|--------|-------|
| `/new [název]` | Založit novou konverzaci |
| `/list` | Zobrazit seznam konverzací |
| `/switch <id>` | Přepnout na jinou konverzaci |
| `/personality` | Zobrazit dostupné personality |
| `/personality <name>` | Změnit personalitu |
| `/personality add` | Přidat novou personalitu |
| `/model` | Zobrazit aktuální model |
| `/model <name>` | Změnit model (např. `anthropic/claude-3.5-sonnet`) |
| `/help` | Nápověda |
| `/exit` | Ukončit agenta |

## Personality

Agent podporuje různé personality, které mění styl odpovědí:

- **default** (Analyst) - Profesionální, věcné odpovědi
- **friendly** (Kamarád Investor) - Přátelský, neformální styl s emoji
- **expert** (Quant Expert) - Detailní technické analýzy
- **contrarian** (Devil's Advocate) - Skeptický pohled, hledá rizika

Personalitu lze změnit příkazem `/personality <name>` nebo přidat vlastní pomocí `/personality add`.

## Příklady dotazů

```
> Najdi akcie s PE < 15 a růstem nad 10%

> Porovnej AAPL, MSFT a NVDA

> Jaký je trend revenue pro TSLA za posledních 8 kvartálů?

> Které akcie v technology sektoru mají nejlepší ROE?

> Analyzuj fundamenty společnosti AMD
```

## Dostupné nástroje

Agent má přístup k následujícím databázovým nástrojům:

1. **query_tickers** - Dotazy na tabulku tickers (ceny, metriky, valuace)
2. **get_ticker_details** - Detailní info o konkrétní akcii
3. **query_time_series** - Historická data (revenue, earnings, margins)
4. **search_tickers** - Vyhledávání podle názvu/sektoru/industry
5. **compare_tickers** - Porovnání více akcií

## Konfigurace

Konfigurace je v souboru `config.yaml`:

```yaml
model: "google/gemini-2.0-flash-001"
active_personality: "default"
personalities:
  custom_name:
    name: "Display Name"
    system_prompt: |
      Váš custom system prompt...
```

## Doporučené modely

| Model | Popis |
|-------|-------|
| `google/gemini-2.0-flash-001` | Rychlý, levný, dobrý tool calling |
| `anthropic/claude-3.5-sonnet` | Vynikající kvalita odpovědí |
| `openai/gpt-4o` | Spolehlivý, kvalitní reasoning |
| `meta-llama/llama-3.1-70b-instruct` | Open source alternativa |

## Poznámky

- Historie konverzací se ukládá do `conversations.json`
- Agent používá OpenRouter API, které podporuje většinu LLM modelů
- SQL dotazy jsou omezeny na SELECT pro bezpečnost
